use std::{path::Path, sync::atomic::AtomicU64};

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use async_trait::async_trait;

use amqprs::tls::TlsAdaptor;
use amqprs::{
    callbacks::DefaultConnectionCallback,
    channel::{BasicPublishArguments, Channel, ConfirmSelectArguments},
    connection::{Connection, OpenConnectionArguments},
    security::SecurityCredentials,
    BasicProperties,
};

use crate::amqp_fs::{
    descriptor::{ParsingError, WriteError},
    publisher::{Endpoint, Publisher},
};

use crate::amqp_fs::rabbit::{
    message::{AmqpHeaders, Message},
    options::RabbitMessageOptions,
};

/// A [Endpoint] that emits message using a fixed exchange
pub struct AmqpRsExchange {
    /// Connection to the RabbitMQ server
    connection: Connection,
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

impl AmqpRsExchange {
    /// Create a new `RabbitExchnage` endpoint that will write to the
    /// given exchnage. All certificate files must be in PEM form.
    pub fn new(
        // client_cert: &str,
        // client_private_key: &str,
        // root_ca_cert: &str,
        rabbit_addr: &url::Url,
        credentials: SecurityCredentials,
        tls: TlsAdaptor,
        exchange: &str,
        line_opts: RabbitMessageOptions,
    ) -> Self {
        let handle = tokio::runtime::Handle::current();
        let _ = handle.enter();
        Self {
            connection: futures::executor::block_on(async {
                let args = OpenConnectionArguments::new(
                    rabbit_addr.host_str().expect("No host name provided"),
                    rabbit_addr.port().unwrap_or(5671),
                    "",
                    "",
                )
                .connection_name("test test")
                .credentials(credentials)
                .tls_adaptor(tls)
                .finish();

                ////////////////////////////////////////////////////////////////
                // everything below should be the same as regular connection
                // open a connection to RabbitMQ server
                let connection = Connection::open(&args).await.unwrap();
                loop {
                    //  If returns Err, user can try again until registration succeed. the docs say
                    if let Ok(()) = connection
                        .register_callback(DefaultConnectionCallback)
                        .await
                    {
                        break;
                    }
                }
                connection
            }),
            exchange: exchange.to_string(),
            line_opts,
        }
    }
}

#[async_trait]
impl Endpoint for AmqpRsExchange {
    type Publisher = AmqpRsPublisher;

    fn from_command_line(args: &crate::cli::Args) -> anyhow::Result<Self> {
        // let tls = TlsAdaptor::with_client_auth(
        //     Some(&args.tls_options.ca_cert.as_ref().unwrap().as_ref()),
        //     &args.tls_options.cert.as_ref().unwrap().as_ref(),
        //     &args.tls_options.key.as_ref().unwrap().as_ref(),
        //     // args.rabbit_addr.to_owned(),
        //     "anise".to_string(),
        // )?;
        let tls = TlsAdaptor::without_client_auth(
            Some(args.tls_options.ca_cert.as_ref().unwrap().as_ref()),
            "localhost".to_string(),
        )?;
        let credentials =
            if let Some(super::super::options::AuthMethod::Plain) = args.rabbit_options.amqp_auth {
                let plain = &args.rabbit_options.plain_auth;
                plain.try_into()?
            } else {
                anyhow::bail!("Only plain authentication is supported");
            };
        Ok(Self::new(
            &args.endpoint_url()?,
            credentials,
            tls,
            &args.exchange,
            args.rabbit_options.clone(),
        ))
    }

    async fn open(&self, path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        let bad_name_err = std::io::ErrorKind::InvalidInput;

        let channel = self.connection.open_channel(None).await?;
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
        let val: amqp_serde::types::ByteArray = bytes.to_vec().try_into().unwrap();
        self.insert(
            key.try_into().unwrap(),
            amqp_serde::types::FieldValue::x(val),
        );
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
                    Err(WriteError::ConfirmFailed(0))
                }
                None => Ok(()),
            },
        }
    }

    async fn basic_publish(&self, line: &[u8], sync: bool) -> Result<usize, WriteError> {
        let message = Message::new(line, &self.line_opts);
        let headers: amqprs::FieldTable = match message.headers() {
            Ok(headers) => headers,
            Err(ParsingError(err)) => {
                return Err(WriteError::ParsingError(ParsingError(err)));
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
        WriteError::EndpointError {
            source: Box::new(err),
            size: 0,
        }
    }
}


impl TryFrom<&crate::amqp_fs::rabbit::options::AmqpPlainAuth> for SecurityCredentials {
    type Error = std::io::Error;

    fn try_from(plain: &crate::amqp_fs::rabbit::options::AmqpPlainAuth) -> Result<SecurityCredentials, Self::Error> {
        Ok( SecurityCredentials::new_plain(
            &plain.amqp_user.to_string(),
            &plain.password()?.unwrap_or_default(),
        ))
    }
}
