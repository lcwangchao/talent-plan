use crate::server::Value::Timestamp;
use crate::service::{timestamp, transaction};
use percolator_proto::message::*;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    ts: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let mut ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        ts = ts << 18 | 1;
        if self.ts.fetch_max(ts, Ordering::SeqCst) >= ts {
            ts = self.ts.fetch_add(1, Ordering::SeqCst) + 1;
        }
        Ok(TimestampResponse { timestamp: ts })
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    raw_key: Vec<u8>,
    timestamp: u64,
}

impl Key {
    fn new(raw_key: Vec<u8>, timestamp: u64) -> Self {
        Key { raw_key, timestamp }
    }

    fn get_raw_key(&self) -> &Vec<u8> {
        &self.raw_key
    }

    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Key({}@{})",
            String::from_utf8_lossy(self.raw_key.as_slice()),
            self.timestamp
        ))?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

impl Value {
    fn get_timestamp(&self) -> Option<u64> {
        match self {
            Timestamp(ts) => Some(*ts),
            Value::Vector(_) => None,
        }
    }

    fn get_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Timestamp(_) => None,
            Value::Vector(data) => Some(data),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(pub Vec<u8>, pub Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: &Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let m = self.map_from_column(column);
        let start = match ts_start_inclusive {
            Some(ts) => Key::new(key.clone(), ts),
            None => Key::new(key.clone(), 0),
        };
        let end = match ts_end_inclusive {
            Some(ts) => Key::new(key.clone(), ts),
            None => Key::new(key.clone(), u64::MAX),
        };
        m.range((Included(start), Included(end))).next_back()
    }

    #[inline]
    fn get_key_commit_ts_with_start_ts(&self, key: &Vec<u8>, start_ts: u64) -> Option<u64> {
        let m = self.map_from_column(Column::Write);
        let start = Excluded(Key::new(key.clone(), start_ts));
        let v = m.range((start, Unbounded)).next();
        match v {
            Some((_, Timestamp(ts))) if *ts == start_ts => Some(*ts),
            _ => None,
        }
    }

    #[inline]
    fn read_exact(&self, key: &Vec<u8>, column: Column, ts: u64) -> Option<&Value> {
        let key = Key::new(key.clone(), ts);
        self.map_from_column(column).get(&key)
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: &Vec<u8>, column: Column, ts: u64, value: Value) {
        self.mut_map_from_column(column)
            .insert(Key::new(key.clone(), ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: &Vec<u8>, column: Column, ts: u64) {
        self.mut_map_from_column(column)
            .remove(&Key::new(key.clone(), ts));
    }

    #[inline]
    fn mut_map_from_column(&mut self, column: Column) -> &mut BTreeMap<Key, Value> {
        match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }

    #[inline]
    fn map_from_column(&self, column: Column) -> &BTreeMap<Key, Value> {
        match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }
}

impl Debug for KvTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("LockCf:\n")?;
        for (k, v) in self.lock.iter() {
            f.write_fmt(format_args!("  {:?} => {:?}\n", k, v))?;
        }
        f.write_str("WriteCf:\n")?;
        for (k, v) in self.write.iter() {
            f.write_fmt(format_args!("  {:?} => {:?}\n", k, v))?;
        }
        f.write_str("DataCf:\n")?;
        for (k, v) in self.data.iter() {
            f.write_fmt(format_args!("  {:?} => {:?}\n", k, v))?;
        }
        Ok(())
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        let mut conflict_start_ts = 0u64;
        loop {
            if conflict_start_ts > 0 {
                async_std::task::sleep(Duration::from_nanos(TTL)).await;
                self.back_off_maybe_clean_up_lock(conflict_start_ts, req.key.clone());
            }

            let store = self.data.lock().unwrap();
            let lock = store.read(&req.key, Column::Lock, None, None);
            if lock.is_some() {
                if conflict_start_ts > 0 {
                    return Ok(GetResponse::new_with_err_code(
                        ErrCode::ErrPrewriteLockConflict,
                    ));
                }
                conflict_start_ts = lock.unwrap().0.get_timestamp();
                drop(store);
                continue;
            }

            let w = store.read(&req.key, Column::Write, None, Some(req.start_ts));
            let (start_ts, commit_ts) = match w {
                Some((k, v)) => (v.get_timestamp().unwrap(), k.get_timestamp()),
                None => {
                    return Ok(GetResponse::default());
                }
            };

            let value = store
                .read_exact(&req.key, Column::Data, start_ts)
                .unwrap()
                .get_bytes()
                .unwrap()
                .clone();

            return Ok(GetResponse {
                start_ts,
                commit_ts,
                value,
                ..Default::default()
            });
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let mut store = self.data.lock().unwrap();
        let mut writes: Vec<&KvPair> = Vec::with_capacity(req.writes.len());
        for pair in req.writes.iter() {
            let lock = store.read(&pair.key, Column::Lock, None, None);
            match lock {
                Some((key, _)) => {
                    if key.get_timestamp() != req.start_ts {
                        return Ok(PrewriteResponse::new_with_err_code(
                            ErrCode::ErrPrewriteLockConflict,
                        ));
                    }
                }
                _ => {
                    let w = store.read(&pair.key, Column::Write, Some(req.start_ts), None);
                    if w.is_some() {
                        return Ok(PrewriteResponse::new_with_err_code(
                            ErrCode::ErrOptimisticWriteConflict,
                        ));
                    }
                    writes.push(pair);
                }
            }
        }

        for pair in writes {
            store.write(
                &pair.key,
                Column::Lock,
                req.start_ts,
                Value::Vector(req.primary_key.clone()),
            );
            store.write(
                &pair.key,
                Column::Data,
                req.start_ts,
                Value::Vector(pair.value.clone()),
            )
        }

        Ok(PrewriteResponse::default())
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut store = self.data.lock().unwrap();
        for key in req.keys.iter() {
            let lock = store.read(key, Column::Lock, None, None);
            match lock {
                Some((k, _)) => {
                    if k.get_timestamp() != req.start_ts {
                        return Ok(CommitResponse::new_with_err_code(
                            ErrCode::ErrPrewriteLockLost,
                        ));
                    }
                }
                None => {
                    return Ok(CommitResponse::new_with_err_code(
                        ErrCode::ErrPrewriteLockLost,
                    ));
                }
            }
        }

        for key in req.keys.iter() {
            store.erase(key, Column::Lock, req.start_ts);
            store.write(
                key,
                Column::Write,
                req.commit_ts,
                Value::Timestamp(req.start_ts),
            );
        }

        Ok(CommitResponse::default())
    }

    async fn debug(&self, _: DebugRequest) -> labrpc::Result<DebugResponse> {
        let store = self.data.lock().unwrap();
        Ok(DebugResponse {
            info: format!("{:?}", store),
        })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut store = self.data.lock().unwrap();
        let lock = store.read_exact(&key, Column::Lock, start_ts);
        if lock.is_none() {
            return;
        }

        let lock_val = lock.unwrap();
        let primary_key = lock_val.get_bytes().unwrap().clone();

        let primary_lock = store.read_exact(&primary_key, Column::Lock, start_ts);
        if primary_lock.is_some() {
            store.erase(&primary_key, Column::Lock, start_ts);
            store.erase(&key, Column::Lock, start_ts);
            store.erase(&key, Column::Data, start_ts);
            return;
        }

        let commit_ts_opt = store.get_key_commit_ts_with_start_ts(&primary_key, start_ts);
        if commit_ts_opt.is_none() {
            store.erase(&key, Column::Lock, start_ts);
            store.erase(&key, Column::Data, start_ts);
            return;
        }

        let commit_ts = commit_ts_opt.unwrap();
        store.write(&key, Column::Write, commit_ts, Timestamp(start_ts));
        store.erase(&key, Column::Lock, start_ts);
    }
}
