use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord, ThreadedProducer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use crate::amqp_fs::descriptor::{WriteError, WriteErrorKind};
use crate::amqp_fs::publisher::{Endpoint, Publisher};

use super::topic::{TokioProducerContext, Topic};

/// Un-awaited return type for Kafka sends
type KafkaConfirm =
    tokio::task::JoinHandle<std::result::Result<(i32, i64), (KafkaError, OwnedMessage)>>;

/// Endpoint to publish messages to a Kafka server. Each message will
/// be published to topic based on the filename
pub struct TopicEndpoint {
    /// Internal message publisher
    config: ClientConfig,
}

/// Kafka publisher that writes lines to a fixed topic
pub struct TopicPublisher {
    /// Message publisher cloned from the parent endpoint
    topic: Topic,
    /// List of outstanding messages awaiting confirmation
    conf_needed: Arc<Mutex<Vec<KafkaConfirm>>>,
}

impl std::fmt::Debug for TopicEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEndpoint").finish()
    }
}

impl std::fmt::Debug for TopicPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicPublisher")
            .field("topic_name", &self.topic.name())
            .finish()
    }
}

impl TopicEndpoint {
    /// Create a new endpoint that will publish to the cluster behind
    /// the server at `bootstrap_url`.
    pub(super) fn new(bootstrap_url: &str) -> Self {
        let config = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_url)
            .set("message.timeout.ms", "5000")
            .to_owned();

        tracing::info!(config=?config, "Creating Kafka endpoint");

        Self { config }
    }
}
impl TopicPublisher {
    /// Create a new publisher that will write messages to a fixed
    /// topic.
    fn new(ep: &TopicEndpoint, topic_name: &str) -> KafkaResult<Self> {
        tracing::debug!(topic = topic_name, "Creating publisher for topic");
        let ctx = {
            let mut ctx = TokioProducerContext::default();
            let mut recv = ctx.confirm_recv.take().unwrap();
            let name = topic_name.to_owned();
            tokio::spawn(async move {
                while let Some(delivery) = recv.recv().await {
                    match delivery {
                        Ok((partition, offset)) => tracing::trace!(
                            topic = name,
                            partition = partition,
                            offset = offset,
                            "Got delviery confirmation"
                        ),
                        Err(_) => todo!(),
                    }
                }
            });
            ctx
        };
        let producer: Arc<ThreadedProducer<_>> = Arc::new(ep.config.create_with_context(ctx)?);
        let topic = Topic::new(producer, topic_name);

        Ok(Self {
            topic,
            conf_needed: Arc::new(Mutex::new(vec![])),
        })
    }
}

impl From<KafkaError> for WriteErrorKind {
    fn from(value: KafkaError) -> Self {
        WriteErrorKind::EndpointError {
            source: value.into(),
        }
    }
}

impl From<KafkaError> for WriteError {
    fn from(value: KafkaError) -> Self {
        let kind: WriteErrorKind = value.into();
        kind.into_error(0)
    }
}

#[async_trait]
impl Publisher for TopicPublisher {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        self.topic.flush(Timeout::Never)?;
        Ok(())
    }

    #[tracing::instrument(level="trace", skip(self, line), fields(length=line.len()))]
    async fn basic_publish(
        &self,
        line: &[u8],
        force_sync: bool,
    ) -> Result<usize, crate::amqp_fs::descriptor::WriteError> {
        let line2 = line.to_vec();
        let topic = self.topic.name();
        let record: FutureRecord<str, Vec<u8>> = FutureRecord::to(topic).payload(&line2);
        match self.topic.send_topic(record).await {
            Ok(_) => {}
            Err((e, _msg)) => {
                tracing::error!(error=?e, "Failed to submit line to kakfa");
                return Err(e.into());
            }
        };
        if force_sync {
            self.topic.flush(Timeout::Never)?;
        }

        Ok(line.len())
    }
}

#[async_trait]
impl Endpoint for TopicEndpoint {
    type Publisher = TopicPublisher;

    #[tracing::instrument(skip(self))]
    async fn open(
        &self,
        path: &std::path::Path,
        _flags: u32,
    ) -> Result<Self::Publisher, crate::amqp_fs::descriptor::WriteError> {
        let bad_name_err = std::io::ErrorKind::InvalidInput;

        let topic = path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .file_name()
            .ok_or_else(|| WriteError::from(bad_name_err))?
            .to_str()
            .ok_or_else(|| WriteError::from(bad_name_err))?;

        Ok(TopicPublisher::new(self, topic)?)
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
        let ep = TopicEndpoint::new("localhost:9092").unwrap();
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
