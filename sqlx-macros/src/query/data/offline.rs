use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;
use proc_macro2::Span;
use serde::{Serialize, Serializer};

use sqlx_core::describe::Describe;

use crate::database::DatabaseExt;
use crate::query::{Metadata, QueryData};

#[cfg(feature = "postgres")]
use sqlx_core::postgres::Postgres;

#[cfg(feature = "mysql")]
use sqlx_core::mysql::MySql;

#[cfg(feature = "sqlite")]
use sqlx_core::sqlite::Sqlite;

#[cfg(feature = "mssql")]
use sqlx_core::mssql::Mssql;

static OFFLINE_DATA_CACHE: Lazy<Mutex<HashMap<PathBuf, Arc<dyn DynQueryData>>>> =
    Lazy::new(Default::default);

pub struct SerializeDbName<DB>(PhantomData<DB>);

impl<DB> Default for SerializeDbName<DB> {
    fn default() -> Self {
        SerializeDbName(PhantomData)
    }
}

impl<DB: DatabaseExt> Debug for SerializeDbName<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SerializeDbName")
            .field(&DB::DATABASE_PATH)
            .finish()
    }
}

impl<DB: DatabaseExt> Display for SerializeDbName<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.pad(DB::NAME)
    }
}

impl<DB: DatabaseExt> Serialize for SerializeDbName<DB> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(DB::NAME)
    }
}

#[derive(serde::Deserialize)]
struct RawQueryData {
    db_name: String,
    query: String,
    #[serde(skip)]
    hash: String,
    describe: Box<serde_json::value::RawValue>,
}

impl<DB: DatabaseExt> QueryData<DB>
where
    Describe<DB>: serde::Serialize + serde::de::DeserializeOwned,
{
    pub(in crate::query) fn save_in(
        &self,
        dir: impl AsRef<Path>,
        meta: &Metadata,
        input_span: Span,
    ) -> crate::Result<()> {
        // Output to a temporary directory first, then move/rename it atomically to avoid conflicts.
        let tmp_dir = meta.target_dir.join("sqlx");
        fs::create_dir_all(&tmp_dir).map_err(|e| {
            format!(
                "failed to create temporary offline query directory {}: {:?}",
                tmp_dir.display(),
                e
            )
        })?;

        // We save under the hash of the span representation because that should be unique
        // per invocation.
        let tmp_path = tmp_dir.join(&format!(
            "query-{}.json",
            hash_string(&format!("{:?}", input_span))
        ));
        let mut buf_writer = BufWriter::new(
            File::create(&tmp_path)
                .map_err(|e| format!("failed to open path {}: {}", tmp_path.display(), e))?,
        );
        serde_json::to_writer_pretty(&mut buf_writer, self)?;
        // Explicitly flush to ensure the file is written before attempting to move it.
        buf_writer.flush()?;

        // Renaming is atomic so we don't clash with other invocations trying to write
        // to the same place.
        let final_path = dir.as_ref().join(&format!("query-{}.json", self.hash));
        fs::create_dir_all(&dir).map_err(|e| {
            format!(
                "failed to create offline query directory {}: {:?}",
                dir.as_ref().display(),
                e
            )
        })?;
        fs::rename(&tmp_path, &final_path)
            .map_err(|e| format!("failed to move query data to final destination: {}", e))?;

        Ok(())
    }
}

pub trait DynQueryData: Send + Sync + 'static {
    fn db_name(&self) -> &str;
    fn hash(&self) -> &str;
    fn query(&self) -> &str;

    #[cfg(feature = "postgres")]
    fn to_postgres(&self) -> &QueryData<Postgres> {
        panic!(
            "saved query data was not for {}, it was for {}",
            Postgres::NAME,
            self.db_name()
        )
    }

    #[cfg(feature = "mysql")]
    fn to_mysql(&self) -> &QueryData<MySql> {
        panic!(
            "saved query data was not for {}, it was for {}",
            MySql::NAME,
            self.db_name()
        )
    }

    #[cfg(feature = "sqlite")]
    fn to_sqlite(&self) -> &QueryData<Sqlite> {
        panic!(
            "saved query data was not for {}, it was for {}",
            Sqlite::NAME,
            self.db_name()
        )
    }

    #[cfg(feature = "mssql")]
    fn to_mssql(&self) -> &QueryData<Mssql> {
        panic!(
            "saved query data was not for {}, it was for {}",
            Mssql::NAME,
            self.db_name()
        )
    }
}

/// Loads a query given the path to its "query-<hash>.json" file. Subsequent calls for the same
/// path are retrieved from an in-memory cache.
pub(in crate::query) fn load_query_from_data_file(
    path: impl AsRef<Path>,
    query: &str,
) -> crate::Result<Arc<dyn DynQueryData>> {
    let path = path.as_ref();

    let mut cache = OFFLINE_DATA_CACHE.lock().unwrap();
    if let Some(cached) = cache.get(path).cloned() {
        if query != cached.query() {
            return Err(format!("hash collision for saved query data").into());
        }
        return Ok(cached);
    }

    #[cfg(procmacr2_semver_exempt)]
    {
        let path = path.as_ref().canonicalize()?;
        let path = path.to_str().ok_or_else(|| {
            format!(
                "sqlx-data.json path cannot be represented as a string: {:?}",
                path
            )
        })?;

        proc_macro::tracked_path::path(path);
    }

    let offline_data_contents = fs::read_to_string(path)
        .map_err(|e| format!("failed to read path {}: {}", path.display(), e))?;
    let offline_data: RawQueryData = serde_json::from_str(&offline_data_contents)?;

    if query != offline_data.query {
        return Err(format!("hash collision for saved query data").into());
    }

    macro_rules! to_dyn_data (
            ($($featname:literal, $db:ty);*$(;)?) => {{
                let dyn_data: Arc<dyn DynQueryData> = match &*offline_data.db_name {
                    $(
                        #[cfg(feature = $featname)]
                        <$db as DatabaseExt>::NAME => Arc::new(QueryData {
                            query: offline_data.query,
                            hash: offline_data.hash,
                            db_name: SerializeDbName(PhantomData),
                            describe: serde_json::from_str::<Describe<$db>>(offline_data.describe.get())?,
                        }),
                    )*
                    other => return Err(format!("query data from filesystem used unknown database: {:?}; is the corresponding feature enabled?", other).into())
                };

                dyn_data
            }}
        );

    let dyn_data = to_dyn_data!(
        "postgres", Postgres;
        "mysql", MySql;
        "sqlite", Sqlite;
        "mssql", Mssql;
    );

    let _ = cache.insert(path.to_owned(), dyn_data.clone());

    Ok(dyn_data)
}

macro_rules! impl_dyn_query_data {
    ($($featname:literal, $db:ty, $method:ident);*$(;)?) => {$(
        #[cfg(feature = $featname)]
        impl DynQueryData for QueryData<$db> {
            fn db_name(&self) -> &str {
                <$db as DatabaseExt>::NAME
            }

            fn hash(&self) -> &str {
                &self.hash
            }

            fn query(&self) -> &str {
                &self.query
            }

            fn $method(&self) -> &QueryData<$db> {
                self
            }
        }
    )*}
}

impl_dyn_query_data!(
    "postgres", Postgres, to_postgres;
    "mysql", MySql, to_mysql;
    "sqlite", Sqlite, to_sqlite;
    "mssql", Mssql, to_mssql;
);

/// Compute the SHA-256 digest of a string.
pub(in crate::query) fn hash_string(query: &str) -> String {
    // picked `sha2` because it's already in the dependency tree for both MySQL and Postgres
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(query.as_bytes()))
}
