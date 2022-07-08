use futures::future::BoxFuture;
use crate::{Config, Error, Txn};
use crate::constants::AccessMode;
use crate::errors::{Result};
use rand::prelude::*;

const MAX_RETRY_DELAY: i64 = i64::MAX / 2;

pub struct TransactionExecutor {
    max_retry_time_ms: usize,
    initial_retry_delay_ms: usize,
    multiplier: f32,
    jitter_factor: f32,
    // in_flight_timeout_ids: Vec<String>
}

impl TransactionExecutor {

    pub fn new(config: Config) -> Self {
        Self {
            max_retry_time_ms: config.max_retry_time_ms,
            initial_retry_delay_ms: config.initial_retry_delay_ms,
            multiplier: config.retry_delay_multiplier,
            jitter_factor: config.retry_delay_jitter_factor,
            // in_flight_timeout_ids: vec![]
        }

    }

    pub async fn run_transaction<F> (&self, mut txn: Txn, _access_mode: AccessMode, transaction_work: F) where F: Fn(&'_ mut Txn) -> BoxFuture<'_, Result<()>> {
        let res = transaction_work(&mut txn).await;
        match res {
            Err(E) => {
                if let Error::UnexpectedMessage(e) = E {
                    println!("{}", e);
                    // self.retry_transaction(txn, transaction_work, -1, self.initial_retry_delay_ms as i64).await;
                }
            }
            Ok(_) => { txn.commit().await; }
        }
    }

    pub fn close() {


    }

    // async fn retry_transaction<F>(&self, mut txn: Txn, transaction_work: F, mut start_time: i64, retry_delay_ms: i64) where F: Fn(&'_ mut Txn) -> BoxFuture<'_, Result<()>> {
    //     let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    //     if start_time == -1 {
    //         start_time = current_time;
    //     }
    //
    //     let elapsed_time = current_time - start_time;
    //     let delay_with_jitter_ms = self.compute_delay_with_jitter(retry_delay_ms);
    //     let new_retry_delay_ms = retry_delay_ms as f32 * self.multiplier;
    //     if elapsed_time < self.max_retry_time_ms as i64 {
    //         sleep(Duration::from_millis(delay_with_jitter_ms as u64)).await;
    //         async move {
    //             self.retry_transaction(txn, transaction_work, start_time, new_retry_delay_ms as i64).await;
    //         }.boxed()
    //     }
    // }

    fn compute_delay_with_jitter(&self, mut delay_ms: i64) -> f32 {
        if delay_ms > MAX_RETRY_DELAY {
            delay_ms = MAX_RETRY_DELAY;
        }

        let jitter = delay_ms as f32 * self.jitter_factor;
        let min = delay_ms as f32 - jitter;
        let max = delay_ms as f32 + jitter;
        return rand::thread_rng().gen_range(min..max); // todo - check thread local one like the java impl
    }
}