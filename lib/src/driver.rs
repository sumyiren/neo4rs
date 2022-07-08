use std::sync::Arc;
use crate::config::{Config};
use crate::pool::{create_pool, ConnectionPool};
use crate::session::Session;

pub struct Driver {
    config: Config,
    connection_pool: Arc<ConnectionPool>
}

impl Driver {

    pub async fn new(config: Config) -> Self {
        let connection_pool = Arc::new(create_pool(&config).await);
        Driver {
            config,
            connection_pool
        }
    }

    pub fn create_session(&self) -> Session {
        Session::new(self.config.clone(), self.connection_pool.clone())
    }

    // todo
    pub fn create_async_session(&self) {
    }
}
