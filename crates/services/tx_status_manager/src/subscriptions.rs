use fuel_core_services::stream::BoxStream;

use crate::ports::{P2PPreConfirmationGossipData, TxStatusProvider};

pub(super) struct Subscriptions<T: TxStatusProvider<P2PPreConfirmationGossipData>> {
    pub new_tx_status: T,
}
