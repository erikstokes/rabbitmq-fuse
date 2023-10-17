use std::sync::Arc;
use std::{path::Path, sync::atomic::AtomicU64};

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use tokio::sync::RwLock;

use async_trait::async_trait;

use amqprs::{
    channel::{BasicPublishArguments, Channel, ConfirmSelectArguments},
    security::SecurityCredentials,
    BasicProperties,
};

use crate::amqp_fs::{
    descriptor::{ParsingError, WriteError, WriteErrorKind},
    publisher::{Endpoint, Publisher},
};

use crate::amqp_fs::rabbit::{
    message::{AmqpHeaders, Message},
    options::RabbitMessageOptions,
};

use super::connection::ConnectionPool;

/// A [Endpoint] that emits message using a fixed exchange

pub struct AmqpRsExchange {
    /// Pool of RabbitMQ connections used to open Publishers
    pool: Arc<RwLock<ConnectionPool>>,

    /// Files created from this table will publish to RabbitMQ on this exchange
    exchange: String,

    /// Options controlling how each line is publshed to the server
    line_opts: crate::amqp_fs::rabbit::options::RabbitMessageOptions,
}

/// A [Publisher] that emits messages to a `RabbitMQ` server using a
/// fixed `exchnage` and `routing_key`

pub struct AmqpRsPublisher {
    /// Channel messages will publish on
    channel: Channel,
    /// Exchange message will be published to
    exchange: String,
    /// Routing key message will be published to
    routing_key: String,
    /// Optionss controlling how lines are parsed into message
    line_opts: RabbitMessageOptions,
    /// Delivery confirmation tracker
    tracker: super::returns::AckTracker,
    /// Current delivery tag
    delivery_tag: AtomicU64,
}

impl std::fmt::Debug for AmqpRsExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpRsExchange")
            .field("exchange", &self.exchange)
            .field("line_opts", &self.line_opts)
            .finish()
    }
}

impl std::fmt::Debug for AmqpRsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpRsPublisher")
            .field("channel", &self.channel.channel_id())
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("line_opts", &self.line_opts)
            .field("tracker", &self.tracker)
            .field("delivery_tag", &self.delivery_tag)
            .finish()
    }
}

impl AmqpRsExchange {
    /// Create a new `RabbitExchnage` endpoint that will write to the
    /// given exchnage. All certificate files must be in PEM form.
    pub(crate) fn new(
        pool: ConnectionPool,
        exchange: &str,
        line_opts: RabbitMessageOptions,
    ) -> Self {
        let handle = tokio::runtime::Handle::current();
        let _ = handle.enter();
        Self {
            pool: Arc::new(RwLock::new(pool)),
            exchange: exchange.to_string(),
            line_opts,
        }
    }
}

#[async_trait]
impl Endpoint for AmqpRsExchange {
    type Publisher = AmqpRsPublisher;
    type Options = super::command::Command;

    async fn open(&self, path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        let bad_name_err = std::io::ErrorKind::InvalidInput;

        let channel = self
            .pool
            .as_ref()
            .read()
            .await
            .get()
            .await
            .map_err(|_| WriteErrorKind::EndpointConnectionError.into_error(0))?
            .open_channel(None)
            .await?;
        channel
            .confirm_select(ConfirmSelectArguments { no_wait: false })
            .await?;
        let routing_key = path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .file_name()
            .ok_or_else(|| WriteError::from(bad_name_err))?
            .to_str()
            .ok_or_else(|| WriteError::from(bad_name_err))?;

        let publisher = AmqpRsPublisher {
            channel,
            exchange: self.exchange.clone(),
            routing_key: routing_key.to_owned(),
            line_opts: self.line_opts.clone(),
            tracker: super::returns::AckTracker::default(),
            delivery_tag: 1.into(),
        };
        publisher
            .channel
            .register_callback(publisher.tracker.clone())
            .await?;
        Ok(publisher)
    }
}

impl AmqpHeaders<'_> for amqprs::FieldTable {
    fn insert_bytes(&mut self, key: &str, bytes: &[u8]) {
        use amqprs::ByteArray;
        use amqprs::FieldValue;
        let val: ByteArray = bytes.to_vec().try_into().unwrap();
        self.insert(key.try_into().unwrap(), FieldValue::x(val));
    }
}

#[async_trait]
impl Publisher for AmqpRsPublisher {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        debug!("Blocking for confirms");
        match self.tracker.wait_for_confirms().await {
            Err(e) => Err(e.into()),
            Ok(returns) => match returns {
                Some(_) => {
                    warn!("Found returned messages");
                    Err(WriteErrorKind::ConfirmFailed.into_error(0))
                }
                None => Ok(()),
            },
        }
    }

    async fn basic_publish(&self, line: &[u8], sync: bool) -> Result<usize, WriteError> {
        let message = Message::new(line, &self.line_opts);
        let headers: amqprs::FieldTable = match message.headers() {
            Ok(headers) => headers,
            Err(ParsingError(size)) => {
                return Err(WriteErrorKind::ParsingError.into_error(size));
            }
        };

        let props = BasicProperties::default()
            .with_content_type("utf")
            .with_headers(headers)
            .finish();
        let publish = self
            .channel
            .basic_publish(
                props,
                message.body().to_vec(),
                BasicPublishArguments {
                    exchange: self.exchange.clone(),
                    routing_key: self.routing_key.clone(),
                    mandatory: true,
                    immediate: false,
                },
            )
            .await;
        match publish {
            Ok(_) => {
                let tag = self
                    .delivery_tag
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.tracker.register(tag).await;
                if sync {
                    self.tracker.wait_for_confirms().await?;
                }
                Ok(line.len())
            }
            Err(_) => Err(std::io::ErrorKind::Other.into()),
        }
    }
}

impl From<amqprs::error::Error> for WriteError {
    fn from(err: amqprs::error::Error) -> WriteError {
        let kind = WriteErrorKind::EndpointError {
            source: Box::new(err),
        };
        Self { kind, size: 0 }
    }
}

impl TryFrom<&crate::amqp_fs::rabbit::options::AmqpPlainAuth> for SecurityCredentials {
    type Error = std::io::Error;

    fn try_from(
        plain: &crate::amqp_fs::rabbit::options::AmqpPlainAuth,
    ) -> Result<SecurityCredentials, Self::Error> {
        Ok(SecurityCredentials::new_plain(
            &plain.amqp_user.to_string(),
            &plain.password()?.unwrap_or_default(),
        ))
    }
}
