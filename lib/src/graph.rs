use crate::config::{config, Config};
use crate::errors::*;
use crate::pool::{create_pool, ConnectionPool};
use crate::query::Query;
use crate::stream::RowStream;
use crate::txn::Txn;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::driver::Driver;

/// A neo4j database abstraction
pub struct Graph {
    config: Config,
}

/// Returns a [`Query`] which provides methods like [`Query::param`] to add parameters to the query
pub fn query(q: &str) -> Query {
    Query::new(q.to_owned())
}

impl Graph {
    /// Connects to the database with default configurations
    pub async fn new(uri: &str, user: &str, password: &str) -> Result<Self> {
        let config = config().uri(uri).user(user).password(password).build()?;
        Ok(Graph { config })
    }

    pub async fn new_with_max_connections(uri: &str, user: &str, password: &str, max_connections: usize) -> Result<Self> {
        let config = config().uri(uri).user(user).password(password).max_connections(max_connections).build()?;
        Ok(Graph { config })
    }

    pub async fn create_driver(&self) -> Driver {
        Driver::new(self.config.clone()).await
    }

    // /// Starts a new transaction, all queries that needs to be run/executed within the transaction
    // /// should be executed using either [`Txn::run`] or [`Txn::execute`]
    // pub async fn start_txn(&self) -> Result<Txn> {
    //     let connection = self.pool.get().await?;
    //     Txn::new(self.config.clone(), connection).await
    // }
    //
    // /// Runs a query using a connection from the connection pool, it doesn't return any
    // /// [`RowStream`] as the `run` abstraction discards any stream.
    // ///
    // /// Use [`Graph::run`] for cases where you just want a write operation
    // ///
    // /// use [`Graph::execute`] when you are interested in the result stream
    // pub async fn run(&self, q: Query) -> Result<()> {
    //     let connection = Arc::new(Mutex::new(self.pool.get().await?));
    //     q.run(&self.config, connection).await
    // }
    //
    // /// Executes a query and returns a [`RowStream`]
    // pub async fn execute(&self, q: Query) -> Result<RowStream> {
    //     let connection = Arc::new(Mutex::new(self.pool.get().await?));
    //     q.execute(&self.config, connection).await
    // }
}
