// use futures_channel::oneshot;
use rdkafka::{
    client::DefaultClientContext,
    error::{IsError as _, KafkaError},
    message::{DeliveryResult, ToBytes},
    producer::{
        future_producer::OwnedDeliveryResult, FutureRecord, Partitioner, Producer as _,
        ProducerContext, ThreadedProducer,
    },
    util::Timeout,
    ClientContext, Message as _,
};
use rdkafka_sys as rdsys;
use std::{ffi::CString, sync::Arc};
use tokio::sync::mpsc::{self, error::TrySendError, OwnedPermit};

#[derive(Debug)]
pub struct TokioProducerContext<C: ClientContext + 'static = DefaultClientContext> {
    wrapped_context: C,
    confirm_send: mpsc::Sender<OwnedDeliveryResult>,
    pub confirm_recv: Option<mpsc::Receiver<OwnedDeliveryResult>>,
}

impl<C> Default for TokioProducerContext<C>
where
    C: Default + ClientContext + 'static,
{
    fn default() -> Self {
        Self::with_capacity(Default::default(), 5_000_000)
    }
}

impl<C: ClientContext + 'static> ClientContext for TokioProducerContext<C> {}

impl<C: ClientContext + 'static> TokioProducerContext<C> {
    /// Create a new tokio channel context using a channel with the
    /// given capacity
    pub fn with_capacity(wrapped_context: C, capacity: usize) -> Self {
        let (confirm_send, confirm_recv) = mpsc::channel(capacity);
        Self {
            wrapped_context,
            confirm_send,
            confirm_recv: Some(confirm_recv),
        }
    }
}

impl<C, Part> ProducerContext<Part> for TokioProducerContext<C>
where
    C: ClientContext + 'static,
    Part: Partitioner,
{
    type DeliveryOpaque = Box<OwnedPermit<OwnedDeliveryResult>>;

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        tx: Box<OwnedPermit<OwnedDeliveryResult>>,
    ) {
        let owned_delivery_result = match *delivery_result {
            Ok(ref message) => Ok((message.partition(), message.offset())),
            Err((ref error, ref message)) => Err((error.clone(), message.detach())),
        };
        tx.send(owned_delivery_result);
    }
}

/// Handle around a `rd_kafka_topic_s`. Unlike [`NativeTopic`] this
/// has a lifetime bounding it to its parent [`Client`] and so is
/// always safe to use
pub struct Topic<C: ClientContext + 'static = DefaultClientContext> {
    ptr: *mut rdsys::rd_kafka_topic_s,
    producer: Arc<ThreadedProducer<TokioProducerContext<C>>>,
    name: String,
}

// Safety: These are safe because rdkafka pointers are fully threadsafe
unsafe impl<C: ClientContext + Send + 'static> Send for Topic<C> {}
unsafe impl<C: ClientContext + Send + 'static> Sync for Topic<C> {}

impl<C: ClientContext> Topic<C> {
    /// The name of the topic being published to
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl<C: ClientContext> Drop for Topic<C> {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_topic_destroy(self.ptr);
        }
    }
}

impl<C> Topic<C>
where
    C: ClientContext + 'static, // does it need to be static?
{
    pub fn new(producer: Arc<ThreadedProducer<TokioProducerContext<C>>>, topic: &str) -> Self {
        let name = topic.to_owned();
        let topic_c = CString::new(topic.to_string()).unwrap();
        let client = producer.client().native_ptr();
        let ptr =
            // Safety: The client is a valid pointer as long as the
            // producer's client exists, which it does via the clone
            // below
            unsafe { rdsys::rd_kafka_topic_new(client, topic_c.as_ptr(), std::ptr::null_mut()) };
        Self {
            ptr,
            name,
            producer: producer.clone(),
        }
    }

    pub fn flush(&self, timeout: Timeout) -> rdkafka::error::KafkaResult<()> {
        self.producer.flush(timeout)
    }

    fn send_topic_inner<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
        tx: Box<OwnedPermit<OwnedDeliveryResult>>,
    ) -> Result<(), (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        fn as_bytes(opt: Option<&(impl ?Sized + ToBytes)>) -> (*mut std::os::raw::c_void, usize) {
            match opt.map(ToBytes::to_bytes) {
                None => (std::ptr::null_mut(), 0),
                Some(p) => (p.as_ptr() as *mut std::os::raw::c_void, p.len()),
            }
        }
        let (payload_ptr, payload_len) = as_bytes(record.payload);
        let (key_ptr, key_len) = as_bytes(record.key);
        let opaque_ptr = Box::into_raw(tx);

        let produce_error = unsafe {
            use rdkafka_sys::rd_kafka_vtype_t as T;
            let ret = rdsys::rd_kafka_producev(
                self.producer.client().native_ptr(),
                T::RD_KAFKA_VTYPE_RKT,
                self.ptr,
                //
                T::RD_KAFKA_VTYPE_PARTITION,
                record.partition.unwrap_or(-1),
                T::RD_KAFKA_VTYPE_MSGFLAGS,
                rdsys::RD_KAFKA_MSG_F_COPY,
                T::RD_KAFKA_VTYPE_VALUE,
                payload_ptr,
                payload_len,
                T::RD_KAFKA_VTYPE_KEY,
                key_ptr,
                key_len,
                T::RD_KAFKA_VTYPE_OPAQUE,
                opaque_ptr,
                T::RD_KAFKA_VTYPE_TIMESTAMP,
                record.timestamp.unwrap_or(0),
                // T::RD_KAFKA_VTYPE_HEADERS,
                // record
                //     .headers
                //     .as_ref()
                //     .map_or(std::ptr::null_mut(), OwnedHeaders::ptr),
                T::RD_KAFKA_VTYPE_END,
            );
            ret
        };
        if produce_error.is_error() {
            let tx = unsafe { Box::from_raw(opaque_ptr) };
            // if the publish failed, drop the permit without sending
            // anything
            std::mem::drop(tx);
            Err((KafkaError::MessageProduction(produce_error.into()), record))
        } else {
            // The kafka producer now owns the headers
            std::mem::forget(record.headers);
            Ok(())
        }
    }

    pub async fn send_topic<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<(), (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        assert_eq!(self.name, record.topic);
        let tx = self.producer.context().confirm_send.clone();
        let permit = Box::new(tx.reserve_owned().await.unwrap());
        self.send_topic_inner(record, permit)
    }
}
