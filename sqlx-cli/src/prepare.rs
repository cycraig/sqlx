use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;
use std::{env, fs};

use anyhow::{bail, Context};
use console::style;

use sqlx::any::{AnyConnectOptions, AnyKind};
use sqlx::Connection;

use crate::metadata::Metadata;
use crate::opt::ConnectOpts;

type QueryData = BTreeMap<String, serde_json::Value>;
type JsonObject = serde_json::Map<String, serde_json::Value>;

// TODO: replace with Metadata?
#[derive(Debug)]
pub struct PrepareCtx {
    pub workspace: bool,
    pub cargo: OsString,
    pub cargo_args: Vec<String>,
    pub manifest_dir: PathBuf,
    pub target_dir: PathBuf,
    pub workspace_root: PathBuf,
    pub connect_ops: ConnectOpts,
}

pub fn run(ctx: &PrepareCtx) -> anyhow::Result<()> {
    // Ensure the database server is available.
    crate::connect(connect_opts).await?.close().await?;

    let root = if ctx.workspace {
        &ctx.workspace_root
    } else {
        &ctx.manifest_dir
    };

    run_prepare_step(ctx, &root.join(".sqlx"))?;

    // TODO: print warning if no queries are generated?
    // if data.is_empty() {
    //     println!(
    //         "{} no queries found; please ensure that the `offline` feature is enabled in sqlx",
    //         style("warning:").yellow()
    //     );
    // }

    println!(
        "query data written to `.sqlx` in the current directory; \
         please check this into version control"
    );

    Ok(())
}

pub fn check(ctx: &PrepareCtx) -> anyhow::Result<()> {
    // Ensure the database server is available.
    crate::connect(connect_opts).await?.close().await?;

    // Re-generate and store the queries in a separate directory.
    let cache_dir = ctx.target_dir.join("sqlx");
    run_prepare_step(ctx, &cache_dir)?;

    // TODO: Compare .sqlx to target/sqlx
    // * For files thta are only in the former, raise a warning
    // * For files that are only in the latter, raise an error

    Ok(())
}

fn run_prepare_step(ctx: &PrepareCtx, cache_dir: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        Path::new("Cargo.toml").exists(),
        r#"Failed to read `Cargo.toml`.
hint: This command only works in the manifest directory of a Cargo package."#
    );

    // Clear or create the directory.
    remove_dir_all::ensure_empty_dir(cache_dir)?;

    let output = Command::new(&ctx.cargo)
        .args(&["metadata", "--format-version=1"])
        .output()
        .context("Could not fetch metadata")?;

    let output_str =
        std::str::from_utf8(&output.stdout).context("Invalid `cargo metadata` output")?;
    let metadata: Metadata = output_str.parse()?;

    let mut check_cmd = Command::new(&ctx.cargo);
    if ctx.workspace {
        // Try only triggering a recompile on crates that use `sqlx-macros` falling back to a full
        // clean on error
        match setup_minimal_project_recompile(&cargo, &metadata) {
            Ok(()) => {}
            Err(err) => {
                println!(
                    "Failed minimal recompile setup. Cleaning entire project. Err: {}",
                    err
                );
                let clean_status = Command::new(&cargo).arg("clean").status()?;
                if !clean_status.success() {
                    bail!("`cargo clean` failed with status: {}", clean_status);
                }
            }
        };

        check_cmd.arg("check").args(cargo_args);

        // `cargo check` recompiles on changed rust flags which can be set either via the env var
        // or through the `rustflags` field in `$CARGO_HOME/config` when the env var isn't set.
        // Because of this we only pass in `$RUSTFLAGS` when present
        if let Ok(rustflags) = env::var("RUSTFLAGS") {
            check_command.env("RUSTFLAGS", rustflags);
        }
    } else {
        check_cmd
            .arg("rustc")
            .args(&ctx.cargo_args)
            .arg("--")
            .arg("--emit")
            .arg("dep-info,metadata")
            // set an always-changing cfg so we can consistently trigger recompile
            .arg("--cfg")
            .arg(format!(
                "__sqlx_recompile_trigger=\"{}\"",
                SystemTime::UNIX_EPOCH.elapsed()?.as_millis()
            ))
            .env("CARGO_TARGET_DIR", metadata.target_directory().clone())
            .status()?
    }
    check_cmd
        .env("DATABASE_URL", database_url)
        .env("SQLX_OFFLINE", "false")
        .env("SQLX_OFFLINE_DIR", cache_dir);

    println!("executing {:?}", check_cmd);

    let check_status = check_cmd.status()?;
    if !check_status.success() {
        bail!("`cargo check` failed with status: {}", check_status);
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
struct ProjectRecompileAction {
    // The names of the packages
    clean_packages: Vec<String>,
    touch_paths: Vec<PathBuf>,
}

/// Sets up recompiling only crates that depend on `sqlx-macros`
///
/// This gets a listing of all crates that depend on `sqlx-macros` (direct and transitive). The
/// crates within the current workspace have their source file's mtimes updated while crates
/// outside the workspace are selectively `cargo clean -p`ed. In this way we can trigger a
/// recompile of crates that may be using compile-time macros without forcing a full recompile
fn setup_minimal_project_recompile(cargo: &str, metadata: &Metadata) -> anyhow::Result<()> {
    let ProjectRecompileAction {
        clean_packages,
        touch_paths,
    } = minimal_project_recompile_action(metadata)?;

    for file in touch_paths {
        let now = filetime::FileTime::now();
        filetime::set_file_times(&file, now, now)
            .with_context(|| format!("Failed to update mtime for {:?}", file))?;
    }

    for pkg_id in &clean_packages {
        let clean_status = Command::new(cargo)
            .args(&["clean", "-p", pkg_id])
            .status()?;

        if !clean_status.success() {
            bail!("`cargo clean -p {}` failed", pkg_id);
        }
    }

    Ok(())
}

fn minimal_project_recompile_action(metadata: &Metadata) -> anyhow::Result<ProjectRecompileAction> {
    // Get all the packages that depend on `sqlx-macros`
    let mut sqlx_macros_dependents = BTreeSet::new();
    let sqlx_macros_ids: BTreeSet<_> = metadata
        .entries()
        // We match just by name instead of name and url because some people may have it installed
        // through different means like vendoring
        .filter(|(_, package)| package.name() == "sqlx-macros")
        .map(|(id, _)| id)
        .collect();
    for sqlx_macros_id in sqlx_macros_ids {
        sqlx_macros_dependents.extend(metadata.all_dependents_of(sqlx_macros_id));
    }

    // Figure out which `sqlx-macros` dependents are in the workspace vs out
    let mut in_workspace_dependents = Vec::new();
    let mut out_of_workspace_dependents = Vec::new();
    for dependent in sqlx_macros_dependents {
        if metadata.workspace_members().contains(&dependent) {
            in_workspace_dependents.push(dependent);
        } else {
            out_of_workspace_dependents.push(dependent);
        }
    }

    // In-workspace dependents have their source file's mtime updated. Out-of-workspace get
    // `cargo clean -p <PKGID>`ed
    let files_to_touch: Vec<_> = in_workspace_dependents
        .iter()
        .filter_map(|id| {
            metadata
                .package(id)
                .map(|package| package.src_paths().to_owned())
        })
        .flatten()
        .collect();
    let packages_to_clean: Vec<_> = out_of_workspace_dependents
        .iter()
        .filter_map(|id| {
            metadata
                .package(id)
                .map(|package| package.name().to_owned())
        })
        .collect();

    Ok(ProjectRecompileAction {
        clean_packages: packages_to_clean,
        touch_paths: files_to_touch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::assert_eq;

    #[test]
    fn minimal_project_recompile_action_works() -> anyhow::Result<()> {
        let sample_metadata_path = Path::new("tests")
            .join("assets")
            .join("sample_metadata.json");
        let sample_metadata = std::fs::read_to_string(sample_metadata_path)?;
        let metadata: Metadata = sample_metadata.parse()?;

        let action = minimal_project_recompile_action(&metadata)?;
        assert_eq!(
            action,
            ProjectRecompileAction {
                clean_packages: vec!["sqlx".into()],
                touch_paths: vec![
                    "/home/user/problematic/workspace/b_in_workspace_lib/src/lib.rs".into(),
                    "/home/user/problematic/workspace/c_in_workspace_bin/src/main.rs".into(),
                ],
            }
        );

        Ok(())
    }
}
