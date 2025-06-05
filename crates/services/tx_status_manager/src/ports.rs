use std::ops::DerefMut;
// use std::ops::DerefMut;
use std::pin::Pin;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use fuel_core_types::{
    fuel_tx::Bytes64,
    services::p2p::{
        DelegatePublicKey,
        GossipData,
        GossipsubMessageAcceptance,
        GossipsubMessageInfo,
        NetworkData,
        PreConfirmationMessage,
        ProtocolSignature,
    },
};

pub type P2PPreConfirmationMessage =
    PreConfirmationMessage<DelegatePublicKey, Bytes64, ProtocolSignature>;

pub type P2PPreConfirmationGossipData = GossipData<P2PPreConfirmationMessage>;

pub struct MyReturner<T> {
    pub stream: Pin<Box<dyn Stream<Item= T> + Send + Sync + 'static>>
}

#[async_trait]
impl <T>AsyncReturner for MyReturner<T> {
    type Item = T;

    async fn next(&mut self) -> Option<T> {
        self.stream.next().await
    }
}
#[async_trait]
pub trait AsyncReturner: Send {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
}
#[async_trait]
impl<'a, S: ?Sized + AsyncReturner + Unpin + 'a> AsyncReturner for &'a mut S {
    type Item = S::Item;

    async fn next(&mut self) -> Option<Self::Item> {
        self.next().await
    }
}
#[async_trait]
impl<P> AsyncReturner for Pin<P>
where
    P: DerefMut + Unpin + Sync + Send,
    P::Target: AsyncReturner,
{
    type Item = <P::Target as AsyncReturner>::Item;

    async fn next(&mut self) -> Option<Self::Item> {
        self.deref_mut().next().await
    }
}

#[async_trait]
impl<S: ?Sized + AsyncReturner + Unpin> AsyncReturner for Box<S> {
    type Item = S::Item;
    async fn next(&mut self) -> Option<Self::Item> {
        self.next().await
    }
}


pub trait P2PSubscriptions: Send {
    type GossipedStatuses: NetworkData<P2PPreConfirmationMessage>;

    fn gossiped_tx_statuses(&self) -> impl AsyncReturner<Item = Self::GossipedStatuses> + Send + Sync + 'static;

    /// Report the validity of a transaction received from the network.
    fn notify_gossip_transaction_validity(
        &self,
        message_info: GossipsubMessageInfo,
        validity: GossipsubMessageAcceptance,
    ) -> anyhow::Result<()>;
}
