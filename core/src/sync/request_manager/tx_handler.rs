use metrics::{register_meter_with_group, Meter};
use primitives::{SignedTransaction, TxPropagateId};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use crate::sync::SynchronizationProtocolHandler;
use cfx_types::H256;
lazy_static! {
    static ref TX_FIRST_MISS_METER: Arc<dyn Meter> =
        register_meter_with_group("tx_propagation", "tx_first_miss_size");
    static ref TX_FOR_COMPARE_METER: Arc<dyn Meter> =
        register_meter_with_group("tx_propagation", "tx_for_compare_size");
    static ref TX_RANDOM_BYTE_METER: Arc<dyn Meter> =
        register_meter_with_group("tx_propagation", "tx_random_byte_size");
}
const RECEIVED_TRANSACTION_CONTAINER_WINDOW_SIZE: usize = 64;

struct ReceivedTransactionTimeWindowedEntry {
    pub secs: u64,
    pub tx_ids: Vec<H256>,
}

struct ReceivedTransactionContainerInner {
    window_size: usize,
    slot_duration_as_secs: u64,
    txid_container: Vec<HashSet<TxPropagateId>>,
    time_windowed_indices: Vec<Option<ReceivedTransactionTimeWindowedEntry>>,
}

impl ReceivedTransactionContainerInner {
    pub fn new(window_size: usize, slot_duration_as_secs: u64, peer_num:usize) -> Self {
        let mut time_windowed_indices = Vec::new();
        for _ in 0..window_size {
            time_windowed_indices.push(None);
        }
        let mut container=Vec::new();
        for _i in 0..peer_num{
            container.push(HashSet::new())
        }
        ReceivedTransactionContainerInner {
            window_size,
            slot_duration_as_secs,
            txid_container: container,
            time_windowed_indices,
        }
    }
}

pub struct ReceivedTransactionContainer {
    inner: ReceivedTransactionContainerInner,
}

impl ReceivedTransactionContainer {
    pub fn new(timeout: u64, peer_num: usize) -> Self {
        let slot_duration_as_secs =
            timeout / RECEIVED_TRANSACTION_CONTAINER_WINDOW_SIZE as u64;
        ReceivedTransactionContainer {
            inner: ReceivedTransactionContainerInner::new(
                RECEIVED_TRANSACTION_CONTAINER_WINDOW_SIZE,
                slot_duration_as_secs,
                peer_num
            ),
        }
    }

    pub fn contains_txid(&self, key: &TxPropagateId,nonce : u64) -> bool {
        let inner = &self.inner;
        inner.txid_container[nonce].contains(key)
    }

    pub fn get_length(&self) -> usize { self.inner.txid_container[0].len() }

    pub fn append_transactions(
        &mut self, transactions: Vec<Arc<SignedTransaction>>,
    ) {
        let tx_ids = transactions
            .iter()
            .map(|tx| TxPropagateId::from_slice(tx.hash().as_bytes()))
            .collect::<Vec<_>>();

        let inner = &mut self.inner;

        let now = SystemTime::now();
        let duration = now.duration_since(UNIX_EPOCH);
        let secs = duration.ok().unwrap().as_secs();
        let window_index =
            (secs / inner.slot_duration_as_secs) as usize % inner.window_size;

        let entry = if inner.time_windowed_indices[window_index].is_none() {
            inner.time_windowed_indices[window_index] =
                Some(ReceivedTransactionTimeWindowedEntry {
                    secs,
                    tx_ids: Vec::new(),
                });
            inner.time_windowed_indices[window_index].as_mut().unwrap()
        } else {
            let indices_with_time =
                inner.time_windowed_indices[window_index].as_mut().unwrap();
            if indices_with_time.secs + inner.slot_duration_as_secs <= secs {
                for tx_id in &indices_with_time.tx_ids {
                    for i in 0..inner.txid_container.len(){
                        inner.txid_container[i].remove(&(SynchronizationProtocolHandler::siphash24(i as u64,*tx_id) as u32));
                    }
                }
                indices_with_time.secs = secs;
                indices_with_time.tx_ids = Vec::new();
            }
            indices_with_time
        };

        for tx_id in tx_ids {
                if !self.contains_txid(&(SynchronizationProtocolHandler::siphash24(i as u64,*tx_id) as u32), 0){
                    for i in 0..inner.txid_container.len(){
                        inner.txid_container[i].insert(&(SynchronizationProtocolHandler::siphash24(i as u64,*tx_id) as u32));
                    }
                    entry.tx_ids.push(tx_id);
                }
        }
    }
}

struct SentTransactionContainerInner {
    window_size: usize,
    base_time_tick: usize,
    next_time_tick: usize,
    time_windowed_indices: Vec<Option<Vec<Arc<SignedTransaction>>>>,
}

impl SentTransactionContainerInner {
    pub fn new(window_size: usize) -> Self {
        let mut time_windowed_indices = Vec::new();
        for _ in 0..window_size {
            time_windowed_indices.push(None);
        }

        SentTransactionContainerInner {
            window_size,
            base_time_tick: 0,
            next_time_tick: 0,
            time_windowed_indices,
        }
    }
}

/// This struct is not implemented as thread-safe since
/// currently it is only used under protection of lock
/// on SynchronizationState. Later we may refine the
/// locking design to make it thread-safe.
pub struct SentTransactionContainer {
    inner: SentTransactionContainerInner,
}

impl SentTransactionContainer {
    pub fn new(window_size: usize) -> Self {
        SentTransactionContainer {
            inner: SentTransactionContainerInner::new(window_size),
        }
    }

    pub fn get_transaction(
        &self, window_index: usize, index: usize,
    ) -> Option<Arc<SignedTransaction>> {
        let inner = &self.inner;
        if window_index >= inner.base_time_tick {
            if window_index - inner.base_time_tick >= inner.window_size {
                return None;
            }
        } else {
            if window_index + 1 + std::usize::MAX - inner.base_time_tick
                >= inner.window_size
            {
                return None;
            }
        }

        let transactions = inner.time_windowed_indices
            [window_index % inner.window_size]
            .as_ref();
        if transactions.is_none() {
            return None;
        }

        let transactions = transactions.unwrap();
        if index >= transactions.len() {
            return None;
        }

        Some(transactions[index].clone())
    }

    pub fn append_transactions(
        &mut self, transactions: Vec<Arc<SignedTransaction>>,
    ) -> usize {
        let inner = &mut self.inner;

        let base_window_index = inner.base_time_tick % inner.window_size;
        let next_time_tick = inner.next_time_tick;
        let next_window_index = next_time_tick % inner.window_size;
        inner.time_windowed_indices[next_window_index] = Some(transactions);
        if (next_window_index + 1) % inner.window_size == base_window_index {
            inner.base_time_tick += 1;
        }
        inner.next_time_tick += 1;
        next_time_tick
    }
}
