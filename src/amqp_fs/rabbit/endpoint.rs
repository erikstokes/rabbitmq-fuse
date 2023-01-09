use std::sync::Arc;
use std::path::Path;
use tokio::sync::RwLock;

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use async_trait::async_trait;

use amq_protocol_types::ShortString;
use lapin::{options::{ConfirmSelectOptions, BasicPublishOptions}, BasicProperties};

use crate::amqp_fs::{descriptor::{ParsingError, WriteError},
                     connection::*
};
use super::{message::Message, options::LinePublishOptions};

pub struct RabbitExchnage {
    /// Open RabbitMQ connection
    connection: Arc<RwLock<ConnectionPool>>,

    /// Files created from this table will publish to RabbitMQ on this exchange
    exchange: String,

    line_opts: super::options::LinePublishOptions,

}

impl RabbitExchnage {
    pub fn new(mgr: ConnectionManager,
               exchange: &str,
               line_opts: super::options::LinePublishOptions) -> Self {
        Self {
            connection: Arc::new(RwLock::new(
                crate::amqp_fs::connection::ConnectionPool::builder(mgr).build().unwrap()
            )),
            exchange: exchange.to_string(),
            line_opts,
        }
    }
}

#[async_trait]
impl crate::amqp_fs::publisher::Endpoint for RabbitExchnage {

    type Publisher = RabbitPublisher;

    /// Create a file table from command line arguments
    fn from_command_line(args: &crate::cli::Args) -> Self {
        let conn_props = lapin::ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let connection_manager = crate::amqp_fs::connection::ConnectionManager::from_command_line(args, conn_props);
        Self::new(connection_manager,
                  &args.exchange,
                  args.rabbit_options.clone())
    }

    async fn open(&self, path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        // The file name came out of the existing table, and was
        // validated in `mknod`, so it should still be good here
        let routing_key = path.file_name().unwrap().to_str().unwrap();
        // This is the only place we touch the rabbit connection.
        // Creating channels is not mutating, so we only need read
        // access
        match self.connection.as_ref().read().await.get().await {
            Err(_) => {
                error!("No connection available");
                Err(WriteError::EndpointConnectionError)
            }
            Ok(conn) => {
                let publisher = RabbitPublisher::new(&conn, &self.exchange, routing_key, self.line_opts.clone()).await?;
                debug!(
                    "File publisher for {}/{}",
                    &self.exchange, &routing_key
                );
                Ok(publisher)
            }
        }
    }
}


/// A [Publisher] that emits messages to a RabbitMQ server using a
/// fixed `exchnage` and `routing_key`
pub(crate) struct RabbitPublisher {
    /// RabbitMQ channel the file will publish to on write
    #[doc(hidden)]
    channel: lapin::Channel,

    /// The RabbitMQ exchange lines will be published to
    exchange: String,

    /// The routing key lines will will be published to
    routing_key: String,

    line_opts: LinePublishOptions,

}

impl RabbitPublisher {
    /// Create a new publisher that writes to the given channel
    pub async fn new(
        connection: &lapin::Connection,
        exchange: &str,
        routing_key: &str,
        line_opts: LinePublishOptions,
    ) -> Result<Self, WriteError> {

        // let channel_conf = connection.configuration().clone();
        // channel_conf.set_frame_max(4096);

        let channel = connection.create_channel().await.unwrap();
        info!("Channel {:?} open", channel);

        let out = Self {
            channel,
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            line_opts,
        };

        // debug!("File open sync={}", out.is_sync());

        out.channel
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await
            .expect("Set confirm");
        Ok(out)
    }
}

#[async_trait]
impl crate::amqp_fs::publisher::Publisher for RabbitPublisher {
    /// Wait until all requested publisher confirms have returned
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        debug!("Waiting for pending confirms");
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
                    return Err(WriteError::ConfirmFailed(0));
                }
            }
            Err(err) => {
                return Err(WriteError::RabbitError(err, 0));
            }
        }
        Ok(())
    }


    /// Publish one line of input, returning a promnise for the publisher confirm.
    ///
    /// Returns the number of byte published, or any error returned by
    /// [lapin::Channel::basic_publish]. Note that the final newline is not
    /// publishied, so the return value may be one short of what you
    /// expect.
    async fn basic_publish(&self, line: &[u8], sync: bool) -> Result<usize, WriteError> {
        let pub_opts = BasicPublishOptions {
            mandatory: true,
            immediate: false,
        };
        trace!("publishing line {:?}", String::from_utf8_lossy(line));

        let message = Message::new(line, &self.line_opts);
        let headers = match message.headers() {
            Ok(headers) => headers,
            Err(ParsingError(err)) => {
                return Err(WriteError::ParsingError(ParsingError(err)));
            }
        };

        trace!("headers are {:?}", headers);
        let props = BasicProperties::default()
            .with_content_type(ShortString::from("utf8"))
            .with_headers(headers);

        debug!(
            "Publishing {} bytes to exchange={} routing_key={}",
            line.len(),
            self.exchange,
            self.routing_key
        );
        match self
            .channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                pub_opts,
                message.body(),
                props,
            )
            .await
        {
            Ok(confirm) => {
                debug!("Publish succeeded. Sent {} bytes", line.len());
                if sync {
                    info!("Sync enabled. Blocking for confirm");
                    match confirm.await {
                        Ok(..) => Ok(line.len()),                         // Everything is okay!
                        Err(err) => Err(WriteError::RabbitError(err, 0)), // We at least wrote some stuff, right.. write?
                    }
                } else {
                    Ok(line.len())
                }
            }
            Err(err) => Err(WriteError::RabbitError(err, 0)),
        }
    }
}
