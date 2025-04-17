use labrpc::*;
use std::cmp::min;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;
use std::f32::consts::E;
use std::os::unix::raw::time_t;
use std::thread::sleep;
use std::time::Duration;

use crate::server::Write;
use crate::service::{TSOClient, TransactionClient};
use futures::executor::block_on;
use percolator_proto::message::{
    CommitRequest, CommitResponse, DebugRequest, ErrCode, Error as ProtoError, GetRequest, KvPair,
    PrewriteRequest, PrewriteResponse, ProtoErrorSupported, TimestampRequest,
};
use rayon::prelude::*;
use scopeguard::defer;
use thiserror::Error;

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

#[derive(Clone)]
struct Txn {
    start_ts: u64,
    primary: Option<KvPair>,
    writes: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Txn {
    pub fn new(start_ts: u64) -> Txn {
        assert!(start_ts > 0);
        Txn {
            start_ts,
            primary: None,
            writes: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        assert!(!key.is_empty());
        if self.primary.is_none() {
            self.primary = Some(KvPair { key, value });
        } else {
            self.writes.insert(key, value);
        }
    }

    pub fn get(&self, txn_cli: &TransactionClient, key: Vec<u8>) -> Result<Vec<u8>> {
        assert!(!key.is_empty());
        let resp = block_on(txn_cli.get(&GetRequest {
            start_ts: self.start_ts,
            key,
        }))?;

        match resp.error {
            Some(err) => {
                return Err(Error::Other(err.message));
            }
            None => {}
        }

        Ok(resp.value)
    }

    fn split_writes(&mut self, batch_size: usize) -> Vec<Vec<KvPair>> {
        assert!(batch_size > 0);
        let primary = self.primary.as_ref().unwrap();
        let mut writes = Vec::with_capacity((self.writes.len() + 1) / batch_size + 1);
        let mut current_write = Vec::with_capacity(batch_size);
        current_write.push(primary.clone());
        current_write.push(self.primary.clone().unwrap());
        for (key, value) in self.writes.iter() {
            if current_write.len() >= batch_size {
                writes.push(current_write);
                current_write = Vec::with_capacity(batch_size);
            }
            current_write.push(KvPair {
                key: key.clone(),
                value: value.clone(),
            });
        }
        writes.push(current_write);
        writes
    }

    fn prewrite_batch(
        &self,
        cli: &TransactionClient,
        batch: Vec<KvPair>,
    ) -> Result<PrewriteResponse> {
        block_on(cli.prewrite(&PrewriteRequest {
            start_ts: self.start_ts,
            primary_key: self.primary.as_ref().unwrap().key.clone(),
            writes: batch,
        }))
    }

    fn commit_batch(
        &self,
        cli: &TransactionClient,
        commit_ts: u64,
        is_primary: bool,
        batch: Vec<Vec<u8>>,
    ) -> Result<CommitResponse> {
        block_on(cli.commit(&CommitRequest {
            start_ts: self.start_ts,
            commit_ts,
            is_primary,
            keys: batch,
        }))
    }

    pub fn commit(mut self, tso_cli: &TSOClient, txn_cli: &TransactionClient) -> Result<bool> {
        if self.primary.is_none() {
            return Ok(true);
        }

        let writes = self.split_writes(1);
        let (primary_write, secondary_writes) = writes.split_first().unwrap();

        // prewrite primary
        let mut ok = self
            .prewrite_batch(txn_cli, primary_write.clone())
            .to_bool_result()?;
        if !ok {
            return Ok(false);
        }

        // prewrite secondary writes
        ok = secondary_writes
            .par_iter()
            .map(|writes| {
                self.prewrite_batch(txn_cli, writes.clone())
                    .to_bool_result()
            })
            .reduce_with(|a, b| Ok(a? && b?))
            .unwrap_or(Ok(true))?;
        if !ok {
            return Ok(false);
        }

        // get commit_ts
        let resp = block_on(tso_cli.get_timestamp(&TimestampRequest {}))?;
        let commit_ts = resp.timestamp;

        // commit primary key
        let primary_key = self.primary.as_ref().unwrap().key.clone();
        ok = self
            .commit_batch(txn_cli, commit_ts, true, vec![primary_key])
            .to_bool_result()?;
        if !ok {
            return Ok(false);
        }

        // commit secondary keys
        secondary_writes
            .par_iter()
            .map(|kv| {
                let _ = self.commit_batch(
                    txn_cli,
                    commit_ts,
                    false,
                    kv.iter().map(|kv| kv.key.clone()).collect(),
                );
            })
            .reduce_with(|_, _| ());

        Ok(true)
    }
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso_cli: TSOClient,
    txn_cli: TransactionClient,
    txn: Option<Txn>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_cli: TSOClient, txn_cli: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_cli,
            txn_cli,
            txn: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        BackOffer::new("get_timestamp", || {
            let fut = self.tso_cli.get_timestamp(&TimestampRequest {});
            Ok(block_on(fut)?.timestamp)
        })
        .execute()
    }

    fn create_txn(&self) -> Result<Txn> {
        Ok(Txn::new(self.get_timestamp()?))
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        self.commit().expect("commit previous transaction failed");
        self.txn = Some(self.create_txn().expect("create transaction failed"));
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        assert!(!key.is_empty());
        let txn = match &self.txn {
            Some(txn) => txn,
            None => &self.create_txn()?,
        };
        txn.get(&self.txn_cli, key)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        assert!(!key.is_empty());
        match &mut self.txn {
            Some(txn) => {
                txn.set(key, value);
            }
            None => {
                let mut txn = self.create_txn().expect("create transaction failed");
                txn.set(key, value);
                txn.commit(&self.tso_cli, &self.txn_cli)
                    .expect("commit transaction failed");
            }
        };
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        match self.txn.take() {
            Some(txn) => txn.commit(&self.tso_cli, &self.txn_cli),
            None => Ok(true),
        }
    }

    /// Commits a transaction.
    pub fn kv_debug_info(&mut self) -> Result<String> {
        let resp = block_on(self.txn_cli.debug(&DebugRequest {}))?;
        Ok(resp.info)
    }
}

trait BoolErrorSupportedResult {
    fn to_bool_result(&self) -> Result<bool>;
}

impl<T: ProtoErrorSupported> BoolErrorSupportedResult for Result<T> {
    fn to_bool_result(&self) -> Result<bool> {
        fn is_false_result_code(code: i32) -> bool {
            code == ErrCode::ErrOptimisticWriteConflict as i32
        }

        match self {
            Ok(proto) => match proto.get_err() {
                Some(err) if is_false_result_code(err.code) => Ok(false),
                Some(err) => Err(Error::Other(err.message.clone())),
                None => Ok(true),
            },
            Err(Error::Other(msg)) if msg == "reqhook" => Ok(false),
            Err(err) => Err(err.clone()),
        }
    }
}
