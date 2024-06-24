use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::error::KafkaResult;
use rdkafka::producer::{FutureProducer, FutureRecord, ProducerContext};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext};

use crate::amqp_fs::descriptor::{WriteError, WriteErrorKind};
use crate::amqp_fs::publisher::{Endpoint, Publisher};

#[derive(Debug)]
struct FuseContext {}

impl ProducerContext for FuseContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        todo!()
    }
}

impl ClientContext for FuseContext {}

pub struct KafkaEndpoint {
    producer: FutureProducer,
}

pub struct TopicPublisher {
    producer: FutureProducer,
    topic_name: String,
}

impl std::fmt::Debug for KafkaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEndpoint").finish()
    }
}

impl std::fmt::Debug for TopicPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicPublisher")
            .field("topic_name", &self.topic_name)
            .finish()
    }
}

impl KafkaEndpoint {
    fn new() -> KafkaResult<Self> {
        let config: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer: config })
    }
}
impl TopicPublisher {
    fn new(ep: &KafkaEndpoint, topic_name: &str) -> Self {
        let producer = ep.producer.clone();
        Self {
            producer,
            topic_name: topic_name.to_owned(),
        }
    }
}

#[async_trait]
impl Publisher for TopicPublisher {
    async fn wait_for_confirms(&self) -> Result<(), crate::amqp_fs::descriptor::WriteError> {
        todo!()
    }

    async fn basic_publish(
        &self,
        line: &[u8],
        force_sync: bool,
    ) -> Result<usize, crate::amqp_fs::descriptor::WriteError> {
        let record: FutureRecord<str, [u8]> = FutureRecord::to(&self.topic_name).payload(line);

        let timeout = if force_sync {
            Timeout::Never
        } else {
            Timeout::After(Duration::from_secs(0))
        };
        if let Err((e, _msg)) = self.producer.send(record, timeout).await {
            return Err(WriteErrorKind::EndpointError { source: e.into() }.into_error(0));
        }

        Ok(line.len())
    }
}

#[async_trait]
impl Endpoint for KafkaEndpoint {
    type Publisher = TopicPublisher;

    async fn open(
        &self,
        path: &std::path::Path,
        flags: u32,
    ) -> Result<Self::Publisher, crate::amqp_fs::descriptor::WriteError> {
        let bad_name_err = std::io::ErrorKind::InvalidInput;

        let topic = path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .file_name()
            .ok_or_else(|| WriteError::from(bad_name_err))?
            .to_str()
            .ok_or_else(|| WriteError::from(bad_name_err))?;

        Ok(TopicPublisher::new(self, topic))
    }
}

#[cfg(test)]
mod test {
    use std::{path::PathBuf, time::Duration};

    use rdkafka::{
        message::{Header, OwnedHeaders},
        producer::FutureRecord,
    };

    use super::*;

    #[tokio::test]
    async fn kafka_connect() {
        let ep = KafkaEndpoint::new().unwrap();
        let path: PathBuf = "fuse_test/test.log".into();

        let publisher = ep.open(&path, 0).await.unwrap();
        publisher
            .basic_publish(b"test message", true)
            .await
            .unwrap();

        // let record = FutureRecord::to(topic_name)
        //     .payload("Test Message")
        //     .key("Key")
        //     .headers(OwnedHeaders::new().insert(Header {
        //         key: "header_key",
        //         value: Some("header_value"),
        //     }));
        // ep.producer
        //     .send(record, Duration::from_secs(0))
        //     .await
        //     .unwrap();
    }
}
