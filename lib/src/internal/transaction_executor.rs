use crate::Config;

pub struct TransactionExecutor {
    max_retry_time_ms: u32,
    initial_retry_delay_ms: u32,
    multiplier: u8,
    jitter_factor: u32,
    in_flight_timeout_ids: Vec<String>
}

impl TransactionExecutor {

    pub fn new(config: Config) {

    }
    pub fn execute() {


    }

    pub fn close() {


    }

    fn retryTransaction() {

    }
}