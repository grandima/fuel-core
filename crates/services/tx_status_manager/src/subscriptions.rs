// use fuel_core_services::stream::BoxStream;

use std::pin::Pin;
use crate::ports::{AsyncReturner, P2PPreConfirmationGossipData};

pub(super) struct Subscriptions {
    pub new_tx_status: Pin<Box<dyn AsyncReturner<Item = P2PPreConfirmationGossipData> + Send + Unpin + 'static>>,
}
