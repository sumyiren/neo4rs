use std::sync::{Arc};
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use crate::{config, Config, Query, RowStream, Txn};
use crate::connection::Connection;
use crate::messages::{BoltRequest, BoltResponse};
use crate::pool::ConnectionPool;
use crate::types::BoltList;
use crate::errors::{unexpected, Result};

pub struct Session {
    config: Config,
    connection_pool: Arc<ConnectionPool>
}

impl Session {
    pub fn new(config: Config, connection_pool: Arc<ConnectionPool>) -> Self {
        Session {
            config,
            connection_pool
        }
    }

    pub async fn execute(&self, query: Query) -> Result<()> {
        let mut connection = self.connection_pool.get().await?;
        let run = BoltRequest::run(&self.config.db.clone(), query);
        match connection.send_recv(run).await? {
            BoltResponse::SuccessMessage(_) => {
                match connection.send_recv(BoltRequest::discard()).await? {
                    BoltResponse::SuccessMessage(_) => Ok(()),
                    msg => Err(unexpected(msg, "DISCARD")),
                }
            }
            msg => Err(unexpected(msg, "RUN")),
        }
    }

    pub async fn run(&self, query: Query) -> Result<RowStream> {
        let mut connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
        let run = BoltRequest::run(&self.config.db.clone(), query);
        match connection.clone().lock().await.send_recv(run).await {
            Ok(BoltResponse::SuccessMessage(success)) => {
                let fields: BoltList = success.get("fields").unwrap_or_else(BoltList::new);
                let qid: i64 = success.get("qid").unwrap_or(-1);
                Ok(RowStream::new(
                    qid,
                    fields,
                    self.config.clone().fetch_size,
                    connection.clone(),
                ))
            }
            msg => Err(unexpected(msg, "RUN")),
        }
    }

    // pub async fn run(&self, query: Query) -> Result<()> {
    //     let mut connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
    //     let run = BoltRequest::run(&self.config.db.clone(), query);
    //     match connection.clone().lock().await.send_recv(run).await? {
    //         BoltResponse::SuccessMessage(_) => {
    //             Ok(())
    //         }
    //         msg => Err(unexpected(msg, "RUN")),
    //     }
    // }

    pub async fn begin_transaction(&self) -> Result<Txn> {
        let connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
        Txn::new(self.config.clone(), connection.clone()).await
    }

    // read and write transactions are very similar, but are written like so due to clustering?
    // inspired by https://users.rust-lang.org/t/function-that-takes-an-async-closure/61663/2
    /// auto-commited write transactions - do these perform retries?
    pub async fn write_transaction<F> (&mut self, callback: F) where F: Fn(&'_ mut Txn) -> BoxFuture<'_, ()> {
        let mut txn = self.begin_transaction().await.unwrap();
        callback(&mut txn).await;
        txn.commit().await;
    }

    // pub async fn write_transaction<F> (&mut self, callback: F) where F: Fn(&Txn) {
    //     let txn = self.begin_transaction().await.unwrap();
    //     callback(&txn);
    //     txn.commit();
    //     // let mut result = txn.run(query).await;
    //     // txn.commit();
    //     // result
    //     //         txn.consume_and_commit().await;
    //     // let mut connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
    //     // let run = BoltRequest::run(&self.config.db.clone(), query);
    //     // match connection.clone().lock().await.send_recv(run).await {
    //     //     Ok(BoltResponse::SuccessMessage(success)) => {
    //     //         let fields: BoltList = success.get("fields").unwrap_or_else(BoltList::new);
    //     //         let qid: i64 = success.get("qid").unwrap_or(-1);
    //     //         Ok(RowStream::new(
    //     //             qid,
    //     //             fields,
    //     //             self.config.clone().fetch_size,
    //     //             connection.clone(),
    //     //         ))
    //     //     }
    //     //     msg => Err(unexpected(msg, "RUN")),
    //     // }
    // }

    // pub async fn read_transaction(&self, query: Query) -> Result<RowStream> {
    //     let mut connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
    //     let run = BoltRequest::run(&self.config.db.clone(), query);
    //     match connection.clone().lock().await.send_recv(run).await {
    //         Ok(BoltResponse::SuccessMessage(success)) => {
    //             let fields: BoltList = success.get("fields").unwrap_or_else(BoltList::new);
    //             let qid: i64 = success.get("qid").unwrap_or(-1);
    //             Ok(RowStream::new(
    //                 qid,
    //                 fields,
    //                 self.config.clone().fetch_size,
    //                 connection.clone(),
    //             ))
    //         }
    //         msg => Err(unexpected(msg, "RUN")),
    //     }
    // }
}
