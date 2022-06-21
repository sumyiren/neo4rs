use std::sync::Arc;
use crate::{config, Config, Query, RowStream};
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

    pub async fn run(&self, query: Query) -> Result<()> {
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
}
