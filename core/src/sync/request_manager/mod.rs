use super::{
    synchronization_protocol_handler::{
        ProtocolConfiguration, REQUEST_START_WAITING_TIME,
    },
    synchronization_state::SynchronizationState,
};
use crate::sync::Error;
use cfx_types::H256;
use message::{
    GetBlockHashesByEpoch, GetBlockHeaders, GetBlockTxn, GetBlocks,
    GetCompactBlocks, GetTransactions, TransIndex, TransactionDigests,
};
use metrics::{Gauge, GaugeUsize};
use network::{NetworkContext, PeerId};
use parking_lot::{Mutex, RwLock};
use primitives::{SignedTransaction, TransactionWithSignature, TxPropagateId};
use priority_send_queue::SendQueuePriority;
pub use request_handler::{
    RequestHandler, RequestMessage, SynchronizationPeerRequest,
};
use std::{
    collections::{binary_heap::BinaryHeap, hash_map::Entry, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tx_handler::{ReceivedTransactionContainer, SentTransactionContainer};

mod request_handler;
pub mod tx_handler;

lazy_static! {
    static ref TX_REQUEST_GAUGE: Arc<Gauge<usize>> =
        GaugeUsize::register("tx_diff_set_size");
}
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
enum WaitingRequest {
    Header(H256),
    Block(H256),
    Epoch(u64),
}

/// When a header or block is requested by the `RequestManager`, it is ensured
/// that if it's not fully received, its hash exists
/// in `in_flight` after every function call.
///
/// The thread who removes a hash from `in_flight` is responsible to request it
/// again if it's not received.
///
/// No lock is held when we call another function in this struct, and all locks
/// are acquired in the same order, so there should exist no deadlocks.
// TODO A non-existing block request will remain in the struct forever, and we
// need garbage collect
pub struct RequestManager {
    inflight_requested_transactions: Mutex<HashSet<TxPropagateId>>,
    headers_in_flight: Mutex<HashSet<H256>>,
    header_request_waittime: Mutex<HashMap<H256, Duration>>,
    blocks_in_flight: Mutex<HashSet<H256>>,
    block_request_waittime: Mutex<HashMap<H256, Duration>>,
    epochs_in_flight: Mutex<HashSet<u64>>,

    /// Each element is (timeout_time, request, chosen_peer)
    waiting_requests:
        Mutex<BinaryHeap<(Instant, WaitingRequest, Option<PeerId>)>>,

    /// The following fields are used to control how to
    /// propagate transactions in normal case.
    /// Holds a set of transactions recently sent to this peer to avoid
    /// spamming.
    sent_transactions: RwLock<SentTransactionContainer>,
    pub received_transactions: Arc<RwLock<ReceivedTransactionContainer>>,

    /// This is used to handle request_id matching
    request_handler: Arc<RequestHandler>,
    syn: Arc<SynchronizationState>,
}

impl RequestManager {
    pub fn new(
        protocol_config: &ProtocolConfiguration, syn: Arc<SynchronizationState>,
    ) -> Self {
        let received_tx_index_maintain_timeout =
            protocol_config.received_tx_index_maintain_timeout;

        // FIXME: make sent_transaction_window_size to be 2^pow.
        let sent_transaction_window_size =
            protocol_config.tx_maintained_for_peer_timeout.as_millis()
                / protocol_config.send_tx_period.as_millis();
        Self {
            received_transactions: Arc::new(RwLock::new(
                ReceivedTransactionContainer::new(
                    received_tx_index_maintain_timeout.as_secs(),
                ),
            )),
            inflight_requested_transactions: Default::default(),
            sent_transactions: RwLock::new(SentTransactionContainer::new(
                sent_transaction_window_size as usize,
            )),
            headers_in_flight: Default::default(),
            header_request_waittime: Default::default(),
            blocks_in_flight: Default::default(),
            block_request_waittime: Default::default(),
            epochs_in_flight: Default::default(),
            waiting_requests: Default::default(),
            request_handler: Arc::new(RequestHandler::new(protocol_config)),
            syn,
        }
    }

    pub fn num_epochs_in_flight(&self) -> u64 {
        self.epochs_in_flight.lock().len() as u64
    }

    /// Remove in-flight headers, and headers requested before will be delayed.
    /// If `peer_id` is `None`, all headers will be delayed and `hashes` will
    /// always become empty.
    fn preprocess_header_request(
        &self, hashes: &mut Vec<H256>, peer_id: &Option<PeerId>,
    ) {
        let mut headers_in_flight = self.headers_in_flight.lock();
        let mut header_request_waittime = self.header_request_waittime.lock();
        hashes.retain(|hash| {
            if headers_in_flight.insert(*hash) {
                match header_request_waittime.entry(*hash) {
                    Entry::Vacant(entry) => {
                        entry.insert(*REQUEST_START_WAITING_TIME);
                        if peer_id.is_none() {
                            self.waiting_requests.lock().push((
                                Instant::now() + *REQUEST_START_WAITING_TIME,
                                WaitingRequest::Header(*hash),
                                *peer_id,
                            ));
                            debug!(
                                "Header {:?} request is delayed for later",
                                hash
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Entry::Occupied(mut entry) => {
                        // It is requested before. To prevent possible attacks,
                        // we wait for more time to start
                        // the next request.
                        let t = entry.get_mut();
                        debug!(
                            "Header {:?} is requested again, delay for {:?}",
                            hash, t
                        );
                        self.waiting_requests.lock().push((
                            Instant::now() + *t,
                            WaitingRequest::Header(*hash),
                            *peer_id,
                        ));
                        *t += *REQUEST_START_WAITING_TIME;
                        false
                    }
                }
            } else {
                debug!(
                    "preprocess_header_request: {:?} already in flight",
                    hash
                );
                false
            }
        });
    }

    pub fn request_block_headers(
        &self, io: &NetworkContext, peer_id: Option<PeerId>,
        mut hashes: Vec<H256>,
    )
    {
        self.preprocess_header_request(&mut hashes, &peer_id);
        if hashes.is_empty() {
            debug!("All headers in_flight, skip requesting");
            return;
        }
        self.request_headers_unchecked(io, peer_id.unwrap(), hashes)
    }

    fn request_headers_unchecked(
        &self, io: &NetworkContext, peer_id: PeerId, hashes: Vec<H256>,
    ) {
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id,
            Box::new(RequestMessage::Headers(GetBlockHeaders {
                request_id: 0.into(),
                hashes: hashes.clone(),
            })),
            SendQueuePriority::High,
        ) {
            warn!(
                "Error requesting headers peer={:?} hashes={:?} err={:?}",
                peer_id, hashes, e
            );
            for hash in hashes {
                self.waiting_requests.lock().push((
                    Instant::now() + *REQUEST_START_WAITING_TIME,
                    WaitingRequest::Header(hash),
                    None,
                ));
            }
        } else {
            debug!("Requesting headers peer={:?} hashes={:?}", peer_id, hashes);
        }
    }

    /// Remove in-flight epochs.
    fn preprocess_epoch_request(
        &self, epochs: &mut Vec<u64>, _peer_id: &Option<PeerId>,
    ) {
        let mut epochs_in_flight = self.epochs_in_flight.lock();
        epochs.retain(|epoch_number| epochs_in_flight.insert(*epoch_number));
    }

    pub fn request_epoch_hashes(
        &self, io: &NetworkContext, peer_id: Option<PeerId>,
        mut epochs: Vec<u64>,
    )
    {
        self.preprocess_epoch_request(&mut epochs, &peer_id);
        if epochs.is_empty() {
            debug!("All epochs in_flight, skip requesting");
            return;
        }
        self.request_epochs_unchecked(io, peer_id.unwrap(), epochs)
    }

    fn request_epochs_unchecked(
        &self, io: &NetworkContext, peer_id: PeerId, epochs: Vec<u64>,
    ) {
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id,
            Box::new(RequestMessage::Epochs(GetBlockHashesByEpoch {
                request_id: 0.into(),
                epochs: epochs.clone(),
            })),
            SendQueuePriority::High,
        ) {
            warn!(
                "Error requesting epochs peer={:?} epochs={:?} err={:?}",
                peer_id, epochs, e
            );
            for epoch_number in epochs {
                self.waiting_requests.lock().push((
                    Instant::now() + *REQUEST_START_WAITING_TIME,
                    WaitingRequest::Epoch(epoch_number),
                    None,
                ));
            }
        } else {
            debug!("Requesting epochs peer={:?} epochs={:?}", peer_id, epochs);
        }
    }

    /// Remove in-flight blocks, and blocks requested before will be delayed.
    /// If `peer_id` is `None`, all blocks will be delayed and `hashes` will
    /// always become empty.
    fn preprocess_block_request(
        &self, hashes: &mut Vec<H256>, peer_id: &Option<PeerId>,
    ) {
        let mut blocks_in_flight = self.blocks_in_flight.lock();
        let mut block_request_waittime = self.block_request_waittime.lock();
        hashes.retain(|hash| {
            if blocks_in_flight.insert(*hash) {
                match block_request_waittime.entry(*hash) {
                    Entry::Vacant(entry) => {
                        entry.insert(*REQUEST_START_WAITING_TIME);
                        if peer_id.is_none() {
                            self.waiting_requests.lock().push((
                                Instant::now() + *REQUEST_START_WAITING_TIME,
                                WaitingRequest::Block(*hash),
                                *peer_id,
                            ));
                            debug!(
                                "Block {:?} request is delayed for later",
                                hash
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Entry::Occupied(mut entry) => {
                        // It is requested before. To prevent possible attacks,
                        // we wait for more time to start
                        // the next request.
                        let t = entry.get_mut();
                        debug!(
                            "Block {:?} is requested again, delay for {:?}",
                            hash, t
                        );
                        self.waiting_requests.lock().push((
                            Instant::now() + *t,
                            WaitingRequest::Block(*hash),
                            *peer_id,
                        ));
                        *t += *REQUEST_START_WAITING_TIME;
                        false
                    }
                }
            } else {
                debug!(
                    "preprocess_block_request: {:?} already in flight",
                    hash
                );
                false
            }
        });
    }

    pub fn request_blocks(
        &self, io: &NetworkContext, peer_id: Option<PeerId>,
        mut hashes: Vec<H256>, with_public: bool,
    )
    {
        self.preprocess_block_request(&mut hashes, &peer_id);
        if hashes.is_empty() {
            debug!("All blocks in_flight, skip requesting");
            return;
        }
        self.request_blocks_unchecked(io, peer_id.unwrap(), hashes, with_public)
    }

    fn request_blocks_unchecked(
        &self, io: &NetworkContext, peer_id: PeerId, hashes: Vec<H256>,
        with_public: bool,
    )
    {
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id,
            Box::new(RequestMessage::Blocks(GetBlocks {
                request_id: 0.into(),
                with_public,
                hashes: hashes.clone(),
            })),
            SendQueuePriority::High,
        ) {
            warn!(
                "Error requesting blocks peer={:?} hashes={:?} err={:?}",
                peer_id, hashes, e
            );
            for hash in hashes {
                self.waiting_requests.lock().push((
                    Instant::now() + *REQUEST_START_WAITING_TIME,
                    WaitingRequest::Block(hash),
                    None,
                ));
            }
        } else {
            debug!("Requesting blocks peer={:?} hashes={:?}", peer_id, hashes);
        }
    }

    pub fn request_transactions(
        &self, io: &NetworkContext, peer_id: PeerId,
        transaction_digests: TransactionDigests,
    )
    {
        let window_index: usize = transaction_digests.window_index;
        let (random_byte_vector, fixed_bytes_vector) =
            transaction_digests.get_decomposed_short_ids();
        let random_position: u8 = transaction_digests.random_position;

        if fixed_bytes_vector.is_empty() {
            return;
        }
        let mut inflight_transactions =
            self.inflight_requested_transactions.lock();
        let received_transactions = self.received_transactions.read();

        let (indices, tx_ids) = {
            let mut tx_ids = HashSet::new();
            let mut indices = Vec::new();

            for i in 0..fixed_bytes_vector.len() {
                if received_transactions.contains_txid(
                    fixed_bytes_vector[i],
                    random_byte_vector[i],
                    random_position,
                ) {
                    // Already received
                    continue;
                }

                if !inflight_transactions.insert(fixed_bytes_vector[i]) {
                    // Already being requested
                    continue;
                }

                let index = TransIndex::new((window_index, i));
                indices.push(index);
                tx_ids.insert(fixed_bytes_vector[i]);
            }

            (indices, tx_ids)
        };
        TX_REQUEST_GAUGE.update(tx_ids.len());
        debug!("Request {} tx from peer={}", tx_ids.len(), peer_id);
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id,
            Box::new(RequestMessage::Transactions(GetTransactions {
                request_id: 0.into(),
                indices,
                tx_ids: tx_ids.clone(),
            })),
            SendQueuePriority::Normal,
        ) {
            warn!(
                "Error requesting transactions peer={:?} count={} err={:?}",
                peer_id,
                tx_ids.len(),
                e
            );
            for tx_id in tx_ids {
                inflight_transactions.remove(&tx_id);
            }
        }
    }

    pub fn request_compact_blocks(
        &self, io: &NetworkContext, peer_id: Option<PeerId>,
        mut hashes: Vec<H256>,
    )
    {
        self.preprocess_block_request(&mut hashes, &peer_id);
        if hashes.is_empty() {
            debug!("All blocks in_flight, skip requesting");
            return;
        }
        self.request_compact_block_unchecked(io, peer_id, hashes)
    }

    pub fn request_compact_block_unchecked(
        &self, io: &NetworkContext, peer_id: Option<PeerId>, hashes: Vec<H256>,
    ) {
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id.unwrap(),
            Box::new(RequestMessage::Compact(GetCompactBlocks {
                request_id: 0.into(),
                hashes: hashes.clone(),
            })),
            SendQueuePriority::High,
        ) {
            warn!(
                "Error requesting compact blocks peer={:?} hashes={:?} err={:?}",
                peer_id, hashes, e
            );
            for hash in hashes {
                self.waiting_requests.lock().push((
                    Instant::now() + *REQUEST_START_WAITING_TIME,
                    WaitingRequest::Block(hash),
                    None,
                ));
            }
        } else {
            debug!(
                "Requesting compact blocks peer={:?} hashes={:?}",
                peer_id, hashes
            );
        }
    }

    pub fn request_blocktxn(
        &self, io: &NetworkContext, peer_id: PeerId, block_hash: H256,
        indexes: Vec<usize>,
    )
    {
        if let Err(e) = self.request_handler.send_request(
            io,
            peer_id,
            Box::new(RequestMessage::BlockTxn(GetBlockTxn {
                request_id: 0.into(),
                block_hash: block_hash.clone(),
                indexes: indexes.clone(),
            })),
            SendQueuePriority::High,
        ) {
            warn!(
                "Error requesting blocktxn peer={:?} hash={} err={:?}",
                peer_id, block_hash, e
            );
        } else {
            debug!(
                "Requesting blocktxn peer={:?} hash={}",
                peer_id, block_hash
            );
        }
    }

    pub fn send_request_again(
        &self, io: &NetworkContext, request: &RequestMessage,
    ) {
        let chosen_peer = self.syn.get_random_peer(&HashSet::new());
        match request {
            RequestMessage::Headers(get_headers) => {
                self.request_block_headers(
                    io,
                    chosen_peer,
                    get_headers.hashes.clone(),
                );
            }
            RequestMessage::Blocks(get_blocks) => {
                self.request_blocks(
                    io,
                    chosen_peer,
                    get_blocks.hashes.clone(),
                    true,
                );
            }
            RequestMessage::Compact(get_compact) => {
                self.request_blocks(
                    io,
                    chosen_peer,
                    get_compact.hashes.clone(),
                    true,
                );
            }
            RequestMessage::BlockTxn(blocktxn) => {
                let mut hashes = Vec::new();
                hashes.push(blocktxn.block_hash);
                self.request_blocks(io, chosen_peer, hashes, true);
            }
            RequestMessage::Epochs(get_epoch_hashes) => {
                self.request_epoch_hashes(
                    io,
                    chosen_peer,
                    get_epoch_hashes.epochs.clone(),
                );
            }
            _ => {}
        }
    }

    pub fn remove_mismatch_request(
        &self, io: &NetworkContext, req: &RequestMessage,
    ) {
        match req {
            RequestMessage::Headers(ref get_headers) => {
                let mut headers_in_flight = self.headers_in_flight.lock();
                for hash in &get_headers.hashes {
                    headers_in_flight.remove(hash);
                }
            }
            RequestMessage::Blocks(ref get_blocks) => {
                let mut blocks_in_flight = self.blocks_in_flight.lock();
                for hash in &get_blocks.hashes {
                    blocks_in_flight.remove(hash);
                }
            }
            RequestMessage::Compact(get_compact) => {
                let mut blocks_in_flight = self.blocks_in_flight.lock();
                for hash in &get_compact.hashes {
                    blocks_in_flight.remove(hash);
                }
            }
            RequestMessage::BlockTxn(ref blocktxn) => {
                self.blocks_in_flight.lock().remove(&blocktxn.block_hash);
            }
            RequestMessage::Transactions(ref get_transactions) => {
                let mut inflight_requested_transactions =
                    self.inflight_requested_transactions.lock();
                for tx_id in &get_transactions.tx_ids {
                    inflight_requested_transactions.remove(tx_id);
                }
            }
            RequestMessage::Epochs(ref get_epoch_hashes) => {
                let mut epochs_in_flight = self.epochs_in_flight.lock();
                for epoch_number in &get_epoch_hashes.epochs {
                    epochs_in_flight.remove(epoch_number);
                }
            }
        }
        self.send_request_again(io, req);
    }

    // Match request with given response.
    // No need to let caller handle request resending.
    pub fn match_request(
        &self, io: &NetworkContext, peer_id: PeerId, request_id: u64,
    ) -> Result<RequestMessage, Error> {
        self.request_handler.match_request(io, peer_id, request_id)
    }

    /// Remove from `headers_in_flight` when a header is received.
    ///
    /// If a request is removed from `req_hashes`, it's the caller's
    /// responsibility to ensure that the removed request either has already
    /// received or will be requested by the caller again.
    pub fn headers_received(
        &self, io: &NetworkContext, req_hashes: HashSet<H256>,
        mut received_headers: HashSet<H256>,
    )
    {
        debug!(
            "headers_received: req_hashes={:?} received_headers={:?}",
            req_hashes, received_headers
        );
        let missing_headers = {
            let mut headers_in_flight = self.headers_in_flight.lock();
            let mut header_waittime = self.header_request_waittime.lock();
            let mut missing_headers = Vec::new();
            for req_hash in &req_hashes {
                if !received_headers.remove(req_hash) {
                    // If `req_hash` is not in `headers_in_flight`, it may has
                    // been received or requested
                    // again by another thread, so we do not need to request it
                    // in that case
                    if headers_in_flight.remove(&req_hash) {
                        missing_headers.push(*req_hash);
                    }
                } else {
                    headers_in_flight.remove(req_hash);
                    header_waittime.remove(req_hash);
                }
            }
            for h in &received_headers {
                headers_in_flight.remove(h);
                header_waittime.remove(h);
            }
            missing_headers
        };
        if !missing_headers.is_empty() {
            let chosen_peer = self.syn.get_random_peer(&HashSet::new());
            self.request_block_headers(io, chosen_peer, missing_headers);
        }
    }

    /// Remove from `epochs_in_flight` when a epoch is received.
    pub fn epochs_received(
        &self, io: &NetworkContext, req_epochs: HashSet<u64>,
        mut received_epochs: HashSet<u64>,
    )
    {
        debug!(
            "epochs_received: req_epochs={:?} received_epochs={:?}",
            req_epochs, received_epochs
        );
        let missing_epochs = {
            let mut epochs_in_flight = self.epochs_in_flight.lock();
            let mut missing_epochs = Vec::new();
            for epoch_number in &req_epochs {
                if !received_epochs.remove(epoch_number) {
                    // If `epoch_number` is not in `epochs_in_flight`, it may
                    // has been received or requested
                    // again by another thread, so we do not need to request it
                    // in that case
                    if epochs_in_flight.remove(&epoch_number) {
                        missing_epochs.push(*epoch_number);
                    }
                } else {
                    epochs_in_flight.remove(epoch_number);
                }
            }
            for epoch_number in &received_epochs {
                epochs_in_flight.remove(epoch_number);
            }
            missing_epochs
        };
        if !missing_epochs.is_empty() {
            let chosen_peer = self.syn.get_random_peer(&HashSet::new());
            self.request_epoch_hashes(io, chosen_peer, missing_epochs);
        }
    }

    /// Remove from `blocks_in_flight` when a block is received.
    ///
    /// If a request is removed from `req_hashes`, it's the caller's
    /// responsibility to ensure that the removed request either has already
    /// received or will be requested by the caller again (the case for
    /// `Blocktxn`).
    pub fn blocks_received(
        &self, io: &NetworkContext, req_hashes: HashSet<H256>,
        mut received_blocks: HashSet<H256>, ask_full_block: bool,
        peer: Option<PeerId>, with_public: bool,
    )
    {
        debug!(
            "blocks_received: req_hashes={:?} received_blocks={:?} peer={:?}",
            req_hashes, received_blocks, peer
        );
        let missing_blocks = {
            let mut blocks_in_flight = self.blocks_in_flight.lock();
            let mut block_waittime = self.block_request_waittime.lock();
            let mut missing_blocks = Vec::new();
            for req_hash in &req_hashes {
                if !received_blocks.remove(req_hash) {
                    // If `req_hash` is not in `blocks_in_flight`, it may has
                    // been received or requested
                    // again by another thread, so we do not need to request it
                    // in that case
                    if blocks_in_flight.remove(&req_hash) {
                        missing_blocks.push(*req_hash);
                    }
                } else {
                    blocks_in_flight.remove(req_hash);
                    block_waittime.remove(req_hash);
                }
            }
            for h in &received_blocks {
                blocks_in_flight.remove(h);
                block_waittime.remove(h);
            }
            missing_blocks
        };
        if !missing_blocks.is_empty() {
            // `peer` is passed in for the case that a compact block is received
            // and a full block is reconstructed, but the full block
            // is incorrect. We should ask the same peer for the
            // full block instead of choosing a random peer.
            let chosen_peer =
                peer.or_else(|| self.syn.get_random_peer(&HashSet::new()));
            if ask_full_block {
                self.request_blocks(
                    io,
                    chosen_peer,
                    missing_blocks,
                    with_public,
                );
            } else {
                self.request_compact_blocks(io, chosen_peer, missing_blocks);
            }
        }
    }

    /// We do not need `io` in this function because we do not request missing
    /// transactions
    pub fn transactions_received(
        &self, received_transactions: &HashSet<TxPropagateId>,
    ) {
        let mut inflight_transactions =
            self.inflight_requested_transactions.lock();
        for tx in received_transactions {
            inflight_transactions.remove(tx);
        }
    }

    pub fn get_sent_transactions(
        &self, indices: &Vec<TransIndex>,
    ) -> Vec<TransactionWithSignature> {
        let sent_transactions = self.sent_transactions.read();
        let mut txs = Vec::with_capacity(indices.len());
        for index in indices {
            if let Some(tx) = sent_transactions.get_transaction(index) {
                txs.push(tx.transaction.clone());
            }
        }
        txs
    }

    pub fn append_sent_transactions(
        &self, transactions: Vec<Arc<SignedTransaction>>,
    ) -> usize {
        self.sent_transactions
            .write()
            .append_transactions(transactions)
    }

    pub fn append_received_transactions(
        &self, transactions: Vec<Arc<SignedTransaction>>,
    ) {
        self.received_transactions
            .write()
            .append_transactions(transactions)
    }

    pub fn resend_timeout_requests(&self, io: &NetworkContext) {
        debug!("resend_timeout_requests: start");
        let timeout_requests = self.request_handler.get_timeout_requests(io);
        for req in timeout_requests {
            debug!("Timeout requests: {:?}", req);
            self.remove_mismatch_request(io, &req);
        }
    }

    /// Send waiting requests that their backoff delay have passes
    pub fn resend_waiting_requests(
        &self, io: &NetworkContext, with_public: bool,
    ) {
        debug!("resend_waiting_requests: start");
        let mut headers_waittime = self.header_request_waittime.lock();
        let mut blocks_waittime = self.block_request_waittime.lock();
        let mut waiting_requests = self.waiting_requests.lock();
        let now = Instant::now();
        loop {
            if waiting_requests.is_empty() {
                break;
            }
            let req = waiting_requests.pop().expect("queue not empty");
            if req.0 >= now {
                waiting_requests.push(req);
                break;
            } else {
                let maybe_peer =
                    req.2.or_else(|| self.syn.get_random_peer(&HashSet::new()));
                let chosen_peer = match maybe_peer {
                    Some(p) => p,
                    None => {
                        break;
                    }
                };
                debug!("Send waiting req {:?} to peer={}", req, chosen_peer);

                // Waiting requests are already in-flight, so send them without
                // checking
                match &req.1 {
                    WaitingRequest::Header(h) => {
                        if let Err(e) = self.request_handler.send_request(
                            io,
                            chosen_peer,
                            Box::new(RequestMessage::Headers(
                                GetBlockHeaders {
                                    request_id: 0.into(),
                                    hashes: vec![h.clone()], /* TODO: group
                                                              * multiple hashes
                                                              * into a single
                                                              * request */
                                },
                            )),
                            SendQueuePriority::High,
                        ) {
                            warn!("Error requesting waiting block header peer={:?} hash={} max_blocks={} err={:?}", chosen_peer, h, 1, e);
                            // TODO `h` is got from `waiting_requests`, so it
                            // should
                            // be in `headers_waittime`, and thus we can remove
                            // `or_insert`
                            waiting_requests.push((
                                Instant::now()
                                    + *headers_waittime
                                        .entry(*h)
                                        .and_modify(|t| {
                                            *t += *REQUEST_START_WAITING_TIME
                                        })
                                        .or_insert(*REQUEST_START_WAITING_TIME),
                                WaitingRequest::Header(*h),
                                None,
                            ));
                        }
                    }
                    WaitingRequest::Epoch(n) => {
                        if let Err(e) = self.request_handler.send_request(
                            io,
                            chosen_peer,
                            Box::new(RequestMessage::Epochs(
                                GetBlockHashesByEpoch {
                                    request_id: 0.into(),
                                    epochs: vec![n.clone()], /* TODO: group
                                                              * multiple epochs
                                                              * into a single
                                                              * request */
                                },
                            )),
                            SendQueuePriority::High,
                        ) {
                            warn!("Error requesting waiting epoch peer={:?} epoch_number={} err={:?}", chosen_peer, n, e);
                            waiting_requests.push((
                                Instant::now() + *REQUEST_START_WAITING_TIME,
                                WaitingRequest::Epoch(*n),
                                None,
                            ));
                        }
                    }
                    WaitingRequest::Block(h) => {
                        let blocks = vec![h.clone()];
                        if let Err(e) = self.request_handler.send_request(
                            io,
                            chosen_peer,
                            Box::new(RequestMessage::Blocks(GetBlocks {
                                request_id: 0.into(),
                                with_public,
                                hashes: blocks.clone(),
                            })),
                            SendQueuePriority::High,
                        ) {
                            warn!("Error requesting waiting blocks peer={:?} hashes={:?} err={:?}", chosen_peer, blocks, e);
                            // TODO `blocks` is got from `waiting_requests`, so
                            // it should
                            // be in `blocks_waittime`, and thus we can remove
                            // `or_insert`
                            for hash in blocks {
                                waiting_requests.push((
                                    Instant::now()
                                        + *blocks_waittime
                                            .entry(*h)
                                            .and_modify(|t| {
                                                *t +=
                                                    *REQUEST_START_WAITING_TIME
                                            })
                                            .or_insert(
                                                *REQUEST_START_WAITING_TIME,
                                            ),
                                    WaitingRequest::Block(hash),
                                    None,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn on_peer_connected(&self, peer: PeerId) {
        self.request_handler.add_peer(peer);
    }

    pub fn on_peer_disconnected(&self, io: &NetworkContext, peer: PeerId) {
        if let Some(unfinished_requests) =
            self.request_handler.remove_peer(peer)
        {
            {
                let mut headers_in_flight = self.headers_in_flight.lock();
                let mut header_waittime = self.header_request_waittime.lock();
                let mut blocks_in_flight = self.blocks_in_flight.lock();
                let mut block_waittime = self.block_request_waittime.lock();
                let mut epochs_in_flight = self.epochs_in_flight.lock();
                let mut inflight_transactions =
                    self.inflight_requested_transactions.lock();
                for request in &unfinished_requests {
                    match &**request {
                        RequestMessage::Headers(get_headers) => {
                            for hash in &get_headers.hashes {
                                headers_in_flight.remove(hash);
                                header_waittime.remove(hash);
                            }
                        }
                        RequestMessage::Blocks(get_blocks) => {
                            for hash in &get_blocks.hashes {
                                blocks_in_flight.remove(hash);
                                block_waittime.remove(hash);
                            }
                        }
                        RequestMessage::Compact(get_compact) => {
                            for hash in &get_compact.hashes {
                                blocks_in_flight.remove(hash);
                                block_waittime.remove(hash);
                            }
                        }
                        RequestMessage::BlockTxn(blocktxn) => {
                            blocks_in_flight.remove(&blocktxn.block_hash);
                            block_waittime.remove(&blocktxn.block_hash);
                        }
                        RequestMessage::Transactions(get_transactions) => {
                            for tx_id in &get_transactions.tx_ids {
                                inflight_transactions.remove(tx_id);
                            }
                        }
                        RequestMessage::Epochs(get_epoch_hashes) => {
                            for epoch_number in &get_epoch_hashes.epochs {
                                epochs_in_flight.remove(epoch_number);
                            }
                        }
                    }
                }
            }
            for request in unfinished_requests {
                self.send_request_again(io, &*request);
            }
        } else {
            debug!("Peer already removed form request manager when disconnected peer={}", peer);
        }
    }
}
