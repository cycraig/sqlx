use crate::database::DatabaseExt;
use sqlx_core::database::Database;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Executor;

#[cfg(feature = "offline")]
pub mod offline;

#[cfg_attr(feature = "offline", derive(serde::Serialize))]
#[cfg_attr(
    feature = "offline",
    serde(bound(serialize = "Describe<DB>: serde::Serialize",))
)]
#[derive(Debug)]
pub struct QueryData<DB: DatabaseExt> {
    #[allow(dead_code)]
    pub(super) query: String,
    pub(super) describe: Describe<DB>,
    #[cfg(feature = "offline")]
    pub(super) hash: String,
    #[cfg(feature = "offline")]
    db_name: offline::SerializeDbName<DB>,
}

impl<DB: DatabaseExt> QueryData<DB> {
    pub async fn from_db(
        conn: impl Executor<'_, Database = DB>,
        query: &str,
    ) -> crate::Result<Self> {
        Ok(Self::from_describe(query, conn.describe(query).await?))
    }

    pub fn from_describe(query: &str, describe: Describe<DB>) -> Self {
        QueryData {
            query: query.into(),
            describe,
            #[cfg(feature = "offline")]
            hash: offline::hash_string(query),
            #[cfg(feature = "offline")]
            db_name: offline::SerializeDbName::default(),
        }
    }
}
