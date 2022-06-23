use crate::config::Config;
use crate::errors::{Result, unexpected};
use crate::messages::*;
use crate::pool::*;
use crate::query::*;
use crate::stream::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::types::BoltList;

/// A handle which is used to control a transaction, created as a result of [`Graph::start_txn`]
///
/// When a transation is started, a dedicated connection is resered and moved into the handle which
/// will be released to the connection pool when the [`Txn`] handle is dropped.
pub struct Txn {
    config: Config,
    connection: Arc<Mutex<ManagedConnection>>,
}

impl Txn {
    pub(crate) async fn new(config: Config, mut connection: Arc<Mutex<ManagedConnection>>) -> Result<Self> {
        let begin = BoltRequest::begin();
        match connection.lock().await.send_recv(begin).await? {
            BoltResponse::SuccessMessage(_) => Ok(Txn {
                config,
                connection: connection.clone(),
            }),
            msg => Err(unexpected(msg, "BEGIN")),
        }
    }

    // /// Runs multiple queries one after the other in the same connection
    // pub async fn run_queries(&self, queries: Vec<Query>) -> Result<()> {
    //     for query in queries.into_iter() {
    //         self.run(query).await?;
    //     }
    //     Ok(())
    // }

    // /// Runs a single query and discards the stream.
    // pub async fn run(&self, q: Query) -> Result<()> {
    //     q.run(&self.config, self.connection.clone()).await
    // }

    pub async fn run(&self, query: Query) -> Result<RowStream> {
        let run = BoltRequest::run(&self.config.db.clone(), query);
        match self.connection.clone().lock().await.send_recv(run).await {
            Ok(BoltResponse::SuccessMessage(success)) => {
                let fields: BoltList = success.get("fields").unwrap_or_else(BoltList::new);
                let qid: i64 = success.get("qid").unwrap_or(-1);
                Ok(RowStream::new(
                    qid,
                    fields,
                    self.config.clone().fetch_size,
                    self.connection.clone(),
                ))
            }
            msg => Err(unexpected(msg, "RUN")),
        }
    }

    pub async fn discard_and_commit(&self) -> Result<()> {
        self.discard().await;
        self.commit().await
    }

    pub async fn discard(&self) -> Result<()> {
        match self.connection.clone().lock().await.send_recv(BoltRequest::discard()).await? {
            BoltResponse::SuccessMessage(_) => Ok(()),
            msg => Err(unexpected(msg, "DISCARD")),
        }
    }
    //
    // /// Executes a query and returns a [`RowStream`]
    // pub async fn execute(&self, q: Query) -> Result<RowStream> {
    //     q.execute(&self.config, self.connection.clone()).await
    // }

    /// Commits the transaction in progress
    pub async fn commit(&self) -> Result<()> {
        let commit = BoltRequest::commit();
        match self.connection.lock().await.send_recv(commit).await? {
            BoltResponse::SuccessMessage(_) => Ok(()),
            msg => Err(unexpected(msg, "COMMIT")),
        }
    }

    /// rollback/abort the current transaction
    pub async fn rollback(&self) -> Result<()> {
        let rollback = BoltRequest::rollback();
        match self.connection.lock().await.send_recv(rollback).await? {
            BoltResponse::SuccessMessage(_) => Ok(()),
            msg => Err(unexpected(msg, "ROLLBACK")),
        }
    }
}
