use labrpc::*;
use std::thread::sleep;
use std::time::Duration;

use crate::service::{TSOClient, TransactionClient};
use futures::executor::block_on;
use percolator_proto::message::TimestampRequest;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

struct BackOffer<'a, T, F>
where
    F: Fn() -> Result<T>,
{
    name: &'a str,
    // The time to wait before retrying to send the request.
    backoff_time: u64,
    // The number of times the client has retried sending the request.
    retry_times: usize,
    // The function to call when the client retries sending the request.
    f: F,
}

impl<'a, T, F> BackOffer<'a, T, F>
where
    F: Fn() -> Result<T>,
{
    // Creates a new BackOffer.
    pub fn new(name: &'a str, f: F) -> BackOffer<'a, T, F> {
        BackOffer {
            name,
            backoff_time: BACKOFF_TIME_MS,
            retry_times: 0,
            f,
        }
    }

    // Waits for the backoff time before retrying to send the request.
    pub fn execute(&mut self) -> Result<T> {
        loop {
            let r = (self.f)();
            match r {
                Ok(..) => break r,
                Err(err) => match self.retry_times + 1 {
                    0..RETRY_TIMES => {
                        warn!(
                            "execute {} error: '{}', current retry: {}, retry after: {}ms",
                            self.name, err, self.retry_times, self.backoff_time,
                        );
                        sleep(Duration::from_millis(self.backoff_time));
                        self.backoff_time *= 2;
                        self.retry_times += 1;
                    }
                    _ => break Err(err),
                },
            }
        }
    }
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso: TSOClient,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client { tso }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        BackOffer::new("get_timestamp", || {
            let fut = self.tso.get_timestamp(&TimestampRequest {});
            Ok(block_on(fut)?.timestamp)
        })
        .execute()
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
