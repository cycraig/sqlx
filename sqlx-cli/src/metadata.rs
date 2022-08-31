use std::ffi::OsStr;
use std::{
    collections::{btree_map, BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use anyhow::{Context, Result};
use cargo_metadata::{
    Metadata as CargoMetadata, Package as MetadataPackage, PackageId as MetadataId,
};

/// The minimal amount of package information we care about
///
/// The package's `name` is used to `cargo clean -p` specific crates while the `src_paths` are
/// are used to trigger recompiles of packages within the workspace
#[derive(Debug)]
pub struct Package {
    name: String,
    src_paths: Vec<PathBuf>,
}

impl Package {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn src_paths(&self) -> &[PathBuf] {
        &self.src_paths
    }
}

impl From<&MetadataPackage> for Package {
    fn from(package: &MetadataPackage) -> Self {
        let name = package.name.clone();
        let src_paths = package
            .targets
            .iter()
            .map(|target| target.src_path.clone().into_std_path_buf())
            .collect();

        Self { name, src_paths }
    }
}

/// Contains metadata for the current project
pub struct Metadata {
    /// Maps packages metadata id to the package
    ///
    /// Currently `MetadataId` is used over `PkgId` because pkgid is not a UUID
    packages: BTreeMap<MetadataId, Package>,
    /// All of the crates in the current workspace
    workspace_members: Vec<MetadataId>,
    /// Workspace root path.
    workspace_root: PathBuf,
    /// Maps each dependency to its set of dependents
    reverse_deps: BTreeMap<MetadataId, BTreeSet<MetadataId>>,
    /// The target directory of the project
    ///
    /// Typically `target` at the workspace root, but can be overridden
    target_directory: PathBuf,
}

impl Metadata {
    /// Attempts to parse the manifest from the current working directory using `cargo metadata`.
    pub fn from_current_directory(cargo: &OsStr) -> crate::Result<Self> {
        let output = Command::new(&cargo)
            .args(&["metadata", "--format-version=1"])
            .output()
            .context("Could not fetch metadata")?;

        let output_str =
            std::str::from_utf8(&output.stdout).context("Invalid `cargo metadata` output")?;
        output_str.parse()
    }

    pub fn package(&self, id: &MetadataId) -> Option<&Package> {
        self.packages.get(id)
    }

    pub fn entries<'this>(&'this self) -> btree_map::Iter<'this, MetadataId, Package> {
        self.packages.iter()
    }

    pub fn workspace_members(&self) -> &[MetadataId] {
        &self.workspace_members
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn target_directory(&self) -> &Path {
        &self.target_directory
    }

    /// Gets all dependents (direct and transitive) of `id`
    pub fn all_dependents_of(&self, id: &MetadataId) -> BTreeSet<&MetadataId> {
        let mut dependents = BTreeSet::new();
        self.all_dependents_of_helper(id, &mut dependents);
        dependents
    }

    fn all_dependents_of_helper<'this>(
        &'this self,
        id: &MetadataId,
        dependents: &mut BTreeSet<&'this MetadataId>,
    ) {
        if let Some(immediate_dependents) = self.reverse_deps.get(id) {
            for immediate_dependent in immediate_dependents {
                if dependents.insert(immediate_dependent) {
                    self.all_dependents_of_helper(&immediate_dependent, dependents);
                }
            }
        }
    }
}

impl FromStr for Metadata {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let CargoMetadata {
            packages: metadata_packages,
            workspace_members,
            workspace_root,
            resolve,
            target_directory,
            ..
        } = serde_json::from_str(s).context("failed to parse cargo metadata")?;

        let mut packages = BTreeMap::new();
        for metadata_package in metadata_packages {
            let package = Package::from(&metadata_package);
            packages.insert(metadata_package.id, package);
        }

        let mut reverse_deps: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        let resolve =
            resolve.context("Resolving the dependency graph failed (old version of cargo)")?;
        for node in resolve.nodes {
            for dep in node.deps {
                let dependent = node.id.clone();
                let dependency = dep.pkg;
                reverse_deps
                    .entry(dependency)
                    .or_default()
                    .insert(dependent);
            }
        }

        let workspace_root = workspace_root.into_std_path_buf();
        let target_directory = target_directory.into_std_path_buf();

        Ok(Self {
            packages,
            workspace_members,
            workspace_root,
            reverse_deps,
            target_directory,
        })
    }
}

pub fn manifest_dir(cargo: &OsStr) -> anyhow::Result<PathBuf> {
    let stdout = Command::new(&cargo)
        .args(&["locate-project", "--message-format=plain"])
        .output()
        .context("could not locate manifest directory")?
        .stdout;

    let mut manifest_path: PathBuf = std::str::from_utf8(&stdout)
        .context("output of `cargo locate-project` was not valid UTF-8")?
        // get rid of the trailing newline
        .trim()
        .into();

    manifest_path.pop();
    Ok(manifest_path)
}
