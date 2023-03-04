use std::{collections::VecDeque, sync::{Arc, Mutex, atomic::AtomicU64}};

use tokio::sync::{RwLock, Notify};

use amqprs::{callbacks::ChannelCallback, channel::Channel, CloseChannel, error::Error, Ack, Nack, BasicProperties, Return, Cancel};
use async_trait::async_trait;
use tracing::{debug, error, info, warn, trace};

type Result<T> = std::result::Result<T, Error>;
/// AMQP delivery-tag
type DeliveryTag = amqp_serde::types::LongLongUint; //u64

#[derive(Debug, Default)]
/// Inner implementation for [`AckTracker`]
struct Inner {
    /// delivery-tags that need to be ack'd
    to_ack: RwLock<VecDeque<DeliveryTag>>,
    /// Acks that have been recieved
    acks: RwLock<Vec<Ack>>,
    ack_arrived: Notify,
}

/// Track outstanding and recieved delivery confirmations
#[derive(Clone, Debug, Default)]
pub(super) struct AckTracker {
    inner: Arc<Inner>
}

impl AckTracker {

    /// Add the delivery tag
    pub async fn register(&self, tag: DeliveryTag) {
        debug!("Registering delivery_tag {}", tag);
        self.inner.to_ack.write().await.push_back(tag);
    }

    /// Wait for all outstanding confirms to arrive
    pub async fn wait_for_confirms(&self) -> Result<()> {
        // Pull off the confirms we need at this moment. A write that
        // happens after this doesn't need to be waited on here.
        // let num_needed = self.inner.to_ack.read().await.len();
        let mut needed = {self.inner.to_ack.write().await.split_off(0)};
        loop {
            trace!("Need {} acks", needed.len());
            // Remove all the acks we got from the list of needed ones
            for ack in self.inner.acks.read().await.iter(){
                trace!("Found ack {:?}", ack);
                if ack.mutiple() {
                    while needed.front().unwrap_or(&u64::MAX) <= &ack.delivery_tag() {
                        let tag = needed.pop_back();
                        trace!("confirmed tag {:?}", tag);
                    }
                } else if let Ok(idx) = needed.binary_search(&ack.delivery_tag()) {
                    let tag = needed.remove(idx);
                    trace!("confirmed tag {:?}", tag);
                }
            }
            if needed.is_empty() {
                break
            }
            // We haven't gotten all the confirms we need, so go
            // to sleep and wait for an another to arrive
            trace!("Still need {} acks. Sleeping {:?}", needed.len(), needed);
            self.inner.ack_arrived.notified().await;
        }
        Ok(())
    }

}

#[async_trait]
impl ChannelCallback for AckTracker {
     async fn close(&mut self, channel: &Channel, close: CloseChannel) -> Result<()> {
        error!(
            "handle close request for channel {}, cause: {}",
            channel, close
        );
        Ok(())
    }
    async fn cancel(&mut self, channel: &Channel, cancel: Cancel) -> Result<()> {
        warn!(
            "handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel
        );
        Ok(())
    }
    async fn flow(&mut self, channel: &Channel, active: bool) -> Result<bool> {
        trace!(
            "handle flow request active={} for channel {}",
            active, channel
        );
        Ok(true)
    }
    async fn publish_ack(&mut self, channel: &Channel, ack: Ack) {
        trace!(
            "------------------- handle publish ack delivery_tag={:?} on channel {}",
            ack,
            channel
        );
        self.inner.acks.write().await.push(ack);
        self.inner.ack_arrived.notify_waiters();
        debug!("Have {} acks", self.inner.acks.read().await.len());
    }
    async fn publish_nack(&mut self, channel: &Channel, nack: Nack) {
        warn!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );
    }
    async fn publish_return(
        &mut self,
        channel: &Channel,
        ret: Return,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        warn!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );
    }
}
