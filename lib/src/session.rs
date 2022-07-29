use std::sync::{Arc};
use bolt_client::{Params, Metadata};
use bolt_client::bolt_proto::message::Success;
use std::convert::TryFrom;
use bolt_client::bolt_proto::error::ConversionError;
use futures::future::BoxFuture;
use tokio::sync::Mutex;
// use crate::{Config, Query, RowStream, Txn};
use crate::{Config};
use crate::constants::AccessMode;
use crate::messages::{BoltRequest, BoltResponse};
use crate::pool::ConnectionPool;
use crate::types::BoltList;
use crate::errors::{unexpected, Error};
use std::iter::FromIterator;
// use crate::internal::transaction_executor::TransactionExecutor;

pub struct Session {
    config: Config,
    connection_pool: Arc<ConnectionPool>,
    // transaction_executor: TransactionExecutor,
}


impl Session {
    pub fn new(config: Config, connection_pool: Arc<ConnectionPool>) -> Self {
        Session {
            // transaction_executor: TransactionExecutor::new(config.clone()),
            config,
            connection_pool,
        }
    }

    pub async fn execute(&self, query: String, params: Option<Params>, metadata: Option<Metadata>) -> Result<(), Error> {
        let mut connection = self.connection_pool.get().await?;
        let response = connection.client.run(query, params, metadata).await?;

        match Success::try_from(response) {
            Ok(_) => {
                println!("hereeee");
                let response = connection.client.discard(Some(Metadata::from_iter(vec![("n", -1)]))).await?;
                match Success::try_from(response) {
                    Ok(_) => {
                        Ok(())
                    },
                    Err(err) => {
                        println!("{}", err.to_string());
                        Err(Error::ConversionError)
                    },
                }
            }
            Err(err) => {
                println!("{}", err.to_string());
                Err(Error::ConversionError)
            },
        }
        // match connection.send_recv(run).await? {
        //     BoltResponse::SuccessMessage(_) => {
        //         match connection.send_recv(BoltRequest::discard()).await? {
        //             BoltResponse::SuccessMessage(_) => Ok(()),
        //             msg => Err(unexpected(msg, "DISCARD")),
        //         }
        //     }
        //     msg => Err(unexpected(msg, "RUN")),
        // }
    }

    // pub async fn run(&self, query: Query) -> Result<RowStream> {
    //     let connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
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
    //
    // pub async fn begin_transaction(&self) -> Result<Txn> {
    //     let connection = Arc::new(Mutex::new(self.connection_pool.get().await?));
    //     Txn::new(self.config.clone(), connection.clone()).await
    // }
    //
    // // read and write transactions are very similar, but are written like so due to clustering?
    // // inspired by https://users.rust-lang.org/t/function-that-takes-an-async-closure/61663/2
    // /// auto-commited write transactions - do these perform retries?
    // // no trait alias right now https://stackoverflow.com/questions/44246722/is-there-any-way-to-create-an-alias-of-a-specific-fnmut
    // pub async fn write_transaction<T, F> (&mut self, transaction_work: F) -> Result<T>
    //     where F: Fn(&'_ mut Txn) -> BoxFuture<'_, Result<T>> {
    //     return self.run_transaction::<F, T>(AccessMode::Write, transaction_work).await;
    // }
    //
    // pub async fn read_transaction<T, F> (&mut self, transaction_work: F) -> Result<T>
    //     where F: Fn(&'_ mut Txn) -> BoxFuture<'_, Result<T>> {
    //     return self.run_transaction::<F, T>(AccessMode::Read, transaction_work).await;
    // }
    //
    // async fn run_transaction<F, T> (&self, access_mode: AccessMode, transaction_work: F) -> Result<T>
    //     where F: Fn(&'_ mut Txn) -> BoxFuture<'_, Result<T>> {
    //     let txn = self.begin_transaction().await.unwrap();
    //     return self.transaction_executor.run_transaction::<F, T>(txn, access_mode, transaction_work).await;
    // }


}

// pub async fn write_transaction<F> (&mut self, callback: F) where F: Fn(&'_ mut Txn) -> BoxFuture<'_, ()> {
//     let mut txn = self.begin_transaction().await.unwrap();
//     callback(&mut txn).await;
//     txn.commit().await;

