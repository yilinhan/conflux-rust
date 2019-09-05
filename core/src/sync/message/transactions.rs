// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{
    message::{Message, RequestId},
    sync::{
        message::{
            metrics::TX_HANDLE_TIMER, Context, DynamicCapability, Handleable,
            Key, KeyContainer,
        },
        request_manager::Request,
        Error, ErrorKind, ProtocolConfiguration,
    },
};
use metrics::MeterTimer;
use primitives::{transaction::TxPropagateId, TransactionWithSignature};
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use rlp_derive::{
    RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use std::{any::Any, collections::HashSet, time::Duration};
use elastic_array::core_::num::FpCategory::Nan;

#[derive(Debug, PartialEq, RlpDecodableWrapper, RlpEncodableWrapper)]
pub struct Transactions {
    pub transactions: Vec<TransactionWithSignature>,
}

impl Handleable for Transactions {
    fn handle(self, ctx: &Context) -> Result<(), Error> {
        let transactions = self.transactions;
        debug!(
            "Received {:?} transactions from Peer {:?}",
            transactions.len(),
            ctx.peer
        );

        let peer_info = ctx.manager.syn.get_peer_info(&ctx.peer)?;
        let should_disconnect = {
            let mut peer_info = peer_info.write();
            if peer_info
                .notified_capabilities
                .contains(DynamicCapability::TxRelay(false))
            {
                peer_info.received_transaction_count += transactions.len();
                peer_info.received_transaction_count
                    > ctx
                        .manager
                        .protocol_config
                        .max_trans_count_received_in_catch_up
                        as usize
            } else {
                false
            }
        };

        if should_disconnect {
            bail!(ErrorKind::TooManyTrans);
        }

        let (signed_trans, _) = ctx
            .manager
            .graph
            .consensus
            .txpool
            .insert_new_transactions(transactions);

        ctx.manager
            .request_manager
            .append_received_transactions(signed_trans);

        debug!("Transactions successfully inserted to transaction pool");

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq)]
pub struct GetMini {
    pub request_id: RequestId,
    pub ids: Vec<u64>,
}
impl Request for GetMini {
    fn as_message(&self) -> &dyn Message { self }

    fn as_any(&self) -> &dyn Any { self }

    fn timeout(&self, conf: &ProtocolConfiguration) -> Duration {
        conf.transaction_request_timeout
    }

    fn on_removed(&self, inflight_keys: &KeyContainer) {
        ()
    }

    fn with_inflight(&mut self, inflight_keys: &KeyContainer) {
        ()
    }

    fn is_empty(&self) -> bool { self.ids.is_empty() }

    fn resend(&self) -> Option<Box<dyn Request>> { None }
}

impl Handleable for GetMini{
    fn handle(self, ctx: &Context) -> Result<(),Error>{
        let transactions =ctx.manager.request_manager.mini_get_transactions(&self.ids);
        let response = GetTransactionsResponse {
            request_id: self.request_id.clone(),
            transactions,
        };
        debug!(
            "on_get_mini equest {} txs, returned {} txs",
            self.ids.len(),
            response.transactions.len()
        );

        ctx.send_response(&response)
    }
}

impl Encodable for GetMini {
    fn rlp_append(&self, stream: &mut RlpStream) {
        stream
            .begin_list(2)
            .append(&self.request_id)
            .append_list(&self.ids);
    }
}

impl Decodable for GetMini {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if rlp.item_count()? != 2 {
            return Err(DecoderError::RlpIncorrectListLen);
        }

        Ok(GetMini {
            request_id: rlp.val_at(0)?,
            ids: rlp.list_at(1)?,
        })
    }
}


#[derive(Debug, PartialEq)]
pub struct MinisketchesDigests {
    pub serialized_sketches: Vec<u8>,
}

impl Handleable for MinisketchesDigests{
    fn handle(self, ctx: &Context) -> Result<(), Error> {

        ctx.manager.request_manager.request_mini_transactions(
            ctx.io,
            ctx.peer,
            &self.serialized_sketches,
        );

        Ok(())
    }
}

impl Encodable for MinisketchesDigests {
    fn rlp_append(&self, stream: &mut RlpStream) {
        stream
            .begin_list(1)
            .append(&self.serialized_sketches);
    }
}

impl Decodable for MinisketchesDigests {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        Ok(MinisketchesDigests {
            serialized_sketches: rlp.val_at(0)?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct TransactionDigests {
    pub window_index: usize,
    pub trans_short_ids: Vec<TxPropagateId>,
}

impl Handleable for TransactionDigests {
    fn handle(self, ctx: &Context) -> Result<(), Error> {
        let peer_info = ctx.manager.syn.get_peer_info(&ctx.peer)?;

        let mut peer_info = peer_info.write();
        if peer_info
            .notified_capabilities
            .contains(DynamicCapability::TxRelay(false))
        {
            peer_info.received_transaction_count += self.trans_short_ids.len();
            if peer_info.received_transaction_count
                > ctx
                    .manager
                    .protocol_config
                    .max_trans_count_received_in_catch_up
                    as usize
            {
                bail!(ErrorKind::TooManyTrans);
            }

        }

        ctx.manager.request_manager.request_transactions(
            ctx.io,
            ctx.peer,
            self.window_index,
            &self.trans_short_ids,
        );

        Ok(())
    }
}

impl Encodable for TransactionDigests {
    fn rlp_append(&self, stream: &mut RlpStream) {
        stream
            .begin_list(2)
            .append(&self.window_index)
            .append_list(&self.trans_short_ids);
    }
}

impl Decodable for TransactionDigests {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        Ok(TransactionDigests {
            window_index: rlp.val_at(0)?,
            trans_short_ids: rlp.list_at(1)?,
        })
    }
}

/////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq)]
pub struct GetTransactions {
    pub request_id: RequestId,
    pub window_index: usize,
    pub indices: Vec<usize>,
    pub tx_ids: HashSet<TxPropagateId>,
}

impl Request for GetTransactions {
    fn as_message(&self) -> &dyn Message { self }

    fn as_any(&self) -> &dyn Any { self }

    fn timeout(&self, conf: &ProtocolConfiguration) -> Duration {
        conf.transaction_request_timeout
    }

    fn on_removed(&self, inflight_keys: &KeyContainer) {
        let mut inflight_keys = inflight_keys.write(self.msg_id());
        for tx_id in self.tx_ids.iter() {
            inflight_keys.remove(&Key::Id(*tx_id));
        }
    }

    fn with_inflight(&mut self, inflight_keys: &KeyContainer) {
        let mut inflight_keys = inflight_keys.write(self.msg_id());

        let mut tx_ids: HashSet<TxPropagateId> = HashSet::new();
        for id in self.tx_ids.iter() {
            if inflight_keys.insert(Key::Id(*id)) {
                tx_ids.insert(*id);
            }
        }

        self.tx_ids = tx_ids;
    }

    fn is_empty(&self) -> bool { self.tx_ids.is_empty() }

    fn resend(&self) -> Option<Box<dyn Request>> { None }
}

impl Handleable for GetTransactions {
    fn handle(self, ctx: &Context) -> Result<(), Error> {
        let transactions = ctx
            .manager
            .request_manager
            .get_sent_transactions(self.window_index, &self.indices);
        let response = GetTransactionsResponse {
            request_id: self.request_id,
            transactions,
        };
        debug!(
            "on_get_transactions request {} txs, returned {} txs",
            self.indices.len(),
            response.transactions.len()
        );

        ctx.send_response(&response)
    }
}

impl Encodable for GetTransactions {
    fn rlp_append(&self, stream: &mut RlpStream) {
        stream
            .begin_list(3)
            .append(&self.request_id)
            .append(&self.window_index)
            .append_list(&self.indices);
    }
}

impl Decodable for GetTransactions {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if rlp.item_count()? != 3 {
            return Err(DecoderError::RlpIncorrectListLen);
        }

        Ok(GetTransactions {
            request_id: rlp.val_at(0)?,
            window_index: rlp.val_at(1)?,
            indices: rlp.list_at(2)?,
            tx_ids: HashSet::new(),
        })
    }
}

///////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, RlpDecodable, RlpEncodable)]
pub struct GetTransactionsResponse {
    pub request_id: RequestId,
    pub transactions: Vec<TransactionWithSignature>,
}

impl Handleable for GetTransactionsResponse {
    fn handle(self, ctx: &Context) -> Result<(), Error> {
        let _timer = MeterTimer::time_func(TX_HANDLE_TIMER.as_ref());

        debug!("on_get_transactions_response {:?}", self.request_id);

        let req = ctx.match_request(self.request_id)?;
//        let req = req.downcast_ref::<GetTransactions>(
//            ctx.io,
//            &ctx.manager.request_manager,
//            false,
//        )?;

        // FIXME: Do some check based on transaction request.

        debug!(
            "Received {:?} transactions from Peer {:?}",
            self.transactions.len(),
            ctx.peer
        );

        let (signed_trans, _) = ctx
            .manager
            .graph
            .consensus
            .txpool
            .insert_new_transactions(self.transactions);

        ctx.manager
            .request_manager
            .transactions_received(&(signed_trans.clone().into_iter().map(|tx| tx.hash()).collect()), signed_trans);

        debug!("Transactions successfully inserted to transaction pool");

        Ok(())
    }
}
