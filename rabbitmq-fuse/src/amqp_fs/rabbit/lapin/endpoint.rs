//! `RabbitMQ` [`crate::amqp_fs::publisher::Endpoint`]. The endpoint represents a
//! persistant connection to a server.

use lapin_pool::deadpool::managed::BuildError;
use miette::IntoDiagnostic;
use std::sync::Arc;
use std::{path::Path, sync::Mutex};
use tokio::sync::RwLock;

#[allow(unused_imports)]
use tracing::{debug, error, info, instrument, trace, warn};

use async_trait::async_trait;

use lapin::types::ShortString;
use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions},
    BasicProperties,
};
use lapin_pool::lapin;

use super::{super::message::Message, super::options::RabbitMessageOptions};
use crate::amqp_fs::descriptor::{ParsingError, WriteError, WriteErrorKind};
use crate::amqp_fs::rabbit::message::AmqpHeaders;
use lapin_pool::{pool::ConnectionPool, Opener};

/// Publish messages to the `RabbitMQ` server using a fixed exchange
/// and publisher dependend routing keys
pub struct RabbitExchnage {
    /// Open RabbitMQ connection
    connection: Arc<RwLock<ConnectionPool>>,

    /// Files created from this table will publish to RabbitMQ on this exchange
    exchange: String,

    /// Options controlling how each line is publshed to the server
    line_opts: crate::amqp_fs::rabbit::options::RabbitMessageOptions,
}

impl std::fmt::Debug for RabbitExchnage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitExchnage")
            .field("exchange", &self.exchange)
            .field("line_opts", &self.line_opts)
            .finish()
    }
}

impl RabbitExchnage {
    /// Create a new `RabbitExchnage` endpoint that will write to the given exchnage.
    pub fn new(
        opener: Opener,
        exchange: &str,
        line_opts: crate::amqp_fs::rabbit::options::RabbitMessageOptions,
    ) -> Result<Self, BuildError> {
        Ok(Self {
            connection: Arc::new(RwLock::new(ConnectionPool::builder(opener).build()?)),
            exchange: exchange.to_string(),
            line_opts,
        })
    }

    /// Verify that connections can be opended. Returns Ok if a
    /// connection has been opened.
    pub async fn test_connection(&self) -> miette::Result<()> {
        debug!("Immediate connection requested");
        let _conn = self
            .connection
            .as_ref()
            .read()
            .await
            .get()
            .await
            .into_diagnostic()?;
        Ok(())
    }
}

#[async_trait]
impl crate::amqp_fs::publisher::Endpoint for RabbitExchnage {
    type Publisher = RabbitPublisher;

    /// Open a new publisher writing output to the exchange. The
    /// routing key will be the parent directory component of the path
    async fn open(&self, path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        // The file name came out of the existing table, and was
        // validated in `mknod`, so it should still be good here
        let bad_name_err = std::io::ErrorKind::InvalidInput;
        // Probably want this error, but need an unstable feature,
        // 'io_error_more' first
        // bad_name_err = std::io::ErrorKind::InvalidFilename;

        let routing_key = path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .file_name()
            .ok_or_else(|| WriteError::from(bad_name_err))?
            .to_str()
            .ok_or_else(|| WriteError::from(bad_name_err))?;

        // This is the only place we touch the rabbit connection.
        // Creating channels is not mutating, so we only need read
        // access
        match self.connection.as_ref().read().await.get().await {
            Err(_) => {
                error!("No connection available");
                Err(WriteErrorKind::EndpointConnectionError.into_error(0))
            }
            Ok(conn) => {
                let publisher = RabbitPublisher::new(
                    &conn,
                    &self.exchange,
                    routing_key,
                    self.line_opts.clone(),
                )
                .await?;
                debug!("File publisher for {}/{}", &self.exchange, &routing_key);
                Ok(publisher)
            }
        }
    }
}

/// Recieves confirms as they arrive from the server
#[derive(Debug)]
struct ConfirmPoller {
    // handle: tokio::task::JoinHandle<()>,
    /// The last error returned by the server
    last_error: Arc<Mutex<Option<WriteError>>>,
}

impl ConfirmPoller {
    /// Create a new poller listening on `channel`
    fn new(_channel: &lapin::Channel) -> Self {
        // let channel = channel.clone();
        let last_error = Arc::new(Mutex::new(None));
        // let last_err = last_error.clone();
        Self { last_error }
        // tokio::spawn(async move {
        //     while channel.status().connected() {
        //         ConfirmPoller::check_for_errors(&channel, &last_err).await;
        //     }
        // });
    }

    /// Poll the channel for returned errors
    async fn check_for_errors(channel: &lapin::Channel, last_err: &Arc<Mutex<Option<WriteError>>>) {
        match channel.wait_for_confirms().await {
            Ok(ret) => {
                if !ret.is_empty() {
                    let _ = last_err
                        .lock()
                        .unwrap()
                        .insert(WriteErrorKind::ConfirmFailed.into_error(0));
                }
            }
            Err(e) => {
                error!(error=?e, "Storing error");
                let _ = last_err.lock().unwrap().insert(e.into());
            }
        }
    }
}

impl Drop for RabbitPublisher {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        tokio::spawn(async move {
            if let Err(e) = channel.close(0, "publisher closed").await {
                error!("channel {} failed to close: {}", channel.id(), e);
            }
        });
    }
}

/// A [`crate::amqp_fs::publisher::Publisher`] that emits messages to
/// a `RabbitMQ` server using a fixed `exchnage` and `routing_key`
#[derive(Debug)]
pub(crate) struct RabbitPublisher {
    /// RabbitMQ channel the file will publish to on write
    #[doc(hidden)]
    channel: lapin::Channel,

    /// The RabbitMQ exchange lines will be published to
    exchange: String,

    /// The routing key lines will will be published to
    routing_key: String,

    /// Options to control how individual lines are published
    line_opts: RabbitMessageOptions,

    /// Poller to recieve confirmations as the arrive
    poller: ConfirmPoller,

    /// The last message that was published
    last_write: Mutex<Option<lapin::publisher_confirm::PublisherConfirm>>,
}

impl RabbitPublisher {
    /// Create a new publisher that writes to the given channel
    pub async fn new(
        connection: &lapin::Connection,
        exchange: &str,
        routing_key: &str,
        line_opts: RabbitMessageOptions,
    ) -> Result<Self, WriteError> {
        // let channel_conf = connection.configuration().clone();
        // channel_conf.set_frame_max(4096);

        let channel = connection.create_channel().await.unwrap();
        info!("Channel {:?} open", channel);
        let poller = ConfirmPoller::new(&channel);

        let out = Self {
            channel,
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            line_opts,
            poller,
            last_write: Mutex::new(None),
        };

        // debug!("File open sync={}", out.is_sync());

        out.channel
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await
            .expect("Set confirm");

        Ok(out)
    }

    /// Publish the a RabbitMQ message containing the given `body` and
    /// `properties`. These are published as given. No additional parsing is
    /// done. All messages are published with `mandatory` enabled and
    /// `immediate` disabled. If `sync` is true, the function will not return
    /// until a message confirmation is received.
    #[instrument(level="debug", skip(self, body), fields(length=body.len()))]
    pub async fn basic_publish_with_props(
        &self,
        body: &[u8],
        properties: BasicProperties,
        sync: bool,
    ) -> Result<usize, WriteError> {
        let pub_opts = BasicPublishOptions {
            mandatory: true,
            immediate: false,
        };

        let ret = match self
            .channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                pub_opts,
                body,
                properties,
            )
            .await
        {
            Ok(confirm) => {
                debug!("Publish succeeded. Sent {} bytes", body.len());
                if sync {
                    info!("Sync enabled. Blocking for confirm");
                    match confirm.await {
                        Ok(..) => Ok(body.len()),    // Everything is okay!
                        Err(err) => Err(err.into()), // We at least wrote some stuff, right.. write?
                    }
                } else {
                    self.last_write.lock().unwrap().replace(confirm);
                    debug!(last_write=?self.last_write);
                    Ok(body.len())
                }
            }
            Err(err) => Err(err.into()),
        };

        // Spawn a new task to collect any errors the last publish
        // triggered. We won't see them until the next call to write.
        let err_channel = self.channel.clone();
        let last_err = self.poller.last_error.clone();
        tokio::spawn(async move {
            ConfirmPoller::check_for_errors(&err_channel, &last_err).await;
        });

        ret
    }

    ///Publish one line of input, returning a promnise for the publisher confirm.
    ///
    ///Returns the number of byte published, or any error returned by
    ///[lapin::Channel::basic_publish]. Note that the final newline is not
    ///publishied, so the return value may be one short of what you
    ///expect.
    ///
    ///Based on the value of the [`RabbitMessageOptions::publish_in`] field
    ///of [`Self::line_opts`] this will either json parse the body and insert
    ///it into the headers, or publish the line as bytes in the body.
    pub async fn basic_publish_one_line<Headers>(
        &self,
        line: &[u8],
        sync: bool,
    ) -> Result<usize, WriteError>
    where
        Headers: for<'a> AmqpHeaders<'a> + std::fmt::Debug + Into<lapin::types::FieldTable>,
    {
        if let Some(last_err) = self.poller.last_error.lock().unwrap().take() {
            debug!(error=?last_err, "Found previous error");
            return Err(last_err);
        }

        let message = Message::new(line, &self.line_opts);
        let headers: Headers = match message.headers() {
            Ok(headers) => headers,
            Err(ParsingError(size)) => {
                return Err(ParsingError(size).into());
            }
        };

        let props = BasicProperties::default()
            .with_content_type(ShortString::from("utf8"))
            .with_headers(headers.into());

        debug!(
            "Publishing {} bytes to exchange={} routing_key={}",
            line.len(),
            self.exchange,
            self.routing_key
        );

        self.basic_publish_with_props(message.body(), props, sync)
            .await
    }
}

#[async_trait]
impl crate::amqp_fs::publisher::Publisher for RabbitPublisher {
    fn pop_error(&self) -> Option<WriteError> {
        self.poller.last_error.lock().unwrap().take()
    }

    fn push_error(&self, err: WriteError) {
        let _ret = self.poller.last_error.lock().unwrap().insert(err);
    }

    /// Wait until all requested publisher confirms have returned
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        debug!(channel=?self.channel, "Waiting for pending confirms");

        let last_conf = self.last_write.lock().unwrap().take();
        debug!(confirm = ?last_conf, "Waiting on last confirm");
        if let Some(confirm) = last_conf {
            let ret = confirm.await?;
            debug!(confirmation=?ret);
            if ret.is_nack() {
                error!("Recieved NACK");
                return Err(WriteErrorKind::ConfirmFailed.into_error(0));
            }
            if let Some(msg) = ret.take_message() {
                error!(reply = ?msg.reply_text, delivery_tag=msg.delivery_tag, "Message returned");
                return Err(WriteErrorKind::ConfirmFailed.into_error(0));
            }
        }

        let returned = self.channel.wait_for_confirms().await;
        debug!("Recieved returned messages");
        match returned {
            Ok(all_confs) => {
                if all_confs.is_empty() {
                    debug!("No returns. Everything okay");
                } else {
                    // Some messages were returned to us
                    error!("{} messages not confirmed", all_confs.len());
                    for conf in all_confs {
                        conf.ack(lapin::options::BasicAckOptions::default())
                            .await
                            .expect("Return ack");
                    }
                    return Err(WriteErrorKind::ConfirmFailed.into_error(0));
                }
            }
            Err(err) => {
                return Err(err.into());
            }
        }
        Ok(())
    }

    async fn basic_publish(&self, line: &[u8], sync: bool) -> Result<usize, WriteError> {
        use super::headers::amqp_value_hack::MyFieldTable;
        self.basic_publish_one_line::<MyFieldTable>(line, sync)
            .await
    }
}

impl From<lapin::Error> for WriteError {
    fn from(source: lapin::Error) -> Self {
        let kind = WriteErrorKind::EndpointError {
            source: Box::new(source),
        };
        WriteError { kind, size: 0 }
    }
}
