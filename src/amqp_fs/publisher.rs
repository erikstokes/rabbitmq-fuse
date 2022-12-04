
use std::path::Path;

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};
use async_trait::async_trait;

use super::{descriptor::{WriteError, FileHandle, FHno}, options::{LinePublishOptions, WriteOptions}};

/// Trait that allows parsing and publishing the results of a buffer
/// to a given endpoint
#[async_trait]
pub(crate) trait Publisher: Send+Sync {

     /// Wait until all message to published to the endpoint have been
     /// confirmed. Should return `Ok` if all in-flight messages have
     /// been confrimed, otherwise an error. What exactly "confirmed"
     /// means depends on the endpoint.
    async fn wait_for_confirms(&self) -> Result<(), WriteError>;

    /// Publish one line to the endpoint. This must be implement for
    /// each endpoint type. Publications are not promised to actually
    /// occur, only be scheduled to occur.
    /// [Publisher::wait_for_confirms] should be called to ensure the
    /// publication happened
    async fn basic_publish(&self, line: &[u8], force_sync: bool, line_opts: &LinePublishOptions) -> Result<usize, WriteError>;
}

/// Thing that writes can be published to
#[async_trait]
pub(crate) trait Endpoint: Send+Sync {
 
    /// Construct an endpoint from command-line arguments
    fn from_command_line(args: &crate::cli::Args) -> Self where Self: Sized;

    /// Return a new file handle that allows writing to the endpoint using the endpoint publisher
    async fn open(&self, fd: FHno, path: &Path, flags: u32, opts: &WriteOptions) -> Result<FileHandle, WriteError>;
}

pub(crate) mod rabbit {
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::*;
    use amq_protocol_types::ShortString;
    use lapin::{options::{ConfirmSelectOptions, BasicPublishOptions}, BasicProperties};

    use crate::amqp_fs::{options::{WriteOptions, LinePublishOptions},
                         descriptor::{ParsingError, WriteError},
                         message::Message,
                         connection::*
    };

    pub struct Endpoint {
        /// Open RabbitMQ connection
        connection: Arc<RwLock<ConnectionPool>>,
        
        /// Files created from this table will publish to RabbitMQ on this exchange
        exchange: String,

    }

    impl Endpoint {
        pub fn new(mgr: ConnectionManager, exchange: &str) -> Self {
            Self {
                connection: Arc::new(RwLock::new(
                    crate::amqp_fs::connection::ConnectionPool::builder(mgr).build().unwrap()
                )),
                exchange: exchange.to_string(),
            }
        }
    }

    #[async_trait]
    impl super::Endpoint for Endpoint {

        /// Create a file table from command line arguments
        fn from_command_line(args: &crate::cli::Args) -> Self {
            let conn_props = lapin::ConnectionProperties::default()
                .with_executor(tokio_executor_trait::Tokio::current())
                .with_reactor(tokio_reactor_trait::Tokio);
            let connection_manager = crate::amqp_fs::connection::ConnectionManager::from_command_line(&args, conn_props);
            Self::new(connection_manager, &args.exchange)
        }

        async fn open(&self, fd: FHno, path: &Path, flags: u32, opts: &WriteOptions) -> Result<FileHandle, WriteError> {
            // The file name came out of the existing table, and was
            // validated in `mknod`, so it should still be good here
            let routing_key = path.file_name().unwrap().to_str().unwrap();
            match self.connection.as_ref().read().await.get().await {
                Err(_) => {
                    error!("No connection available");
                    Err(WriteError::EndpointConnectionError)
                } 
                Ok(conn) => {
                    let fhno = fd;
                    let publisher = Box::new(RabbitPublisher::new(&conn, &self.exchange, &routing_key, opts).await?);
                    debug!(
                        "File descriptor {} for {}/{}",
                        fhno, &self.exchange, &routing_key
                    );
                    Ok(FileHandle::new(fhno, publisher, flags, opts.clone()))
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
    }

    impl RabbitPublisher {
        /// Create a new publisher that writes to the given channel
        pub async fn new(
            connection: &lapin::Connection,
            exchange: &str,
            routing_key: &str,
            opts: &WriteOptions,
        ) -> Result<Self, WriteError> {

            // let channel_conf = connection.configuration().clone();
            // channel_conf.set_frame_max(4096);

            let channel = if opts.open_timeout_ms > 0 {
                let channel_open = tokio::time::timeout(
                    tokio::time::Duration::from_millis(opts.open_timeout_ms),
                    connection.create_channel());
                match channel_open.await {
                    Ok(channel) => channel.unwrap(),
                    Err(_) => {
                        error!("Failed to open channel within timeout");
                        return Err(WriteError::TimeoutError(0));
                    }
                }
            } else {
                connection.create_channel().await.unwrap()
            };
            info!("Channel {:?} open", channel);

            let out = Self {
                channel,
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
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
    impl super::Publisher for RabbitPublisher {
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
        async fn basic_publish(&self, line: &[u8], sync: bool, line_opts: &LinePublishOptions) -> Result<usize, WriteError> {
            let pub_opts = BasicPublishOptions {
                mandatory: true,
                immediate: false,
            };
            trace!("publishing line {:?}", String::from_utf8_lossy(line));

            let message = Message::new(line, &line_opts);
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
}
