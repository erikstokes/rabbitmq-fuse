
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use async_trait::async_trait;


use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        Channel, BasicConsumeArguments, BasicPublishArguments, QueueBindArguments, QueueDeclareArguments, ConfirmSelectArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::DefaultConsumer,
    BasicProperties, security::SecurityCredentials,
};
use amqprs::tls::TlsAdaptor;

use crate::amqp_fs::{descriptor::{ParsingError, WriteError}, publisher::{Endpoint, Publisher}};

use super::{options::RabbitMessageOptions, message::{Message, AmqpHeaders}};


pub struct AmqpRsExchange {
    connection: Connection,
    /// Files created from this table will publish to RabbitMQ on this exchange
    exchange: String,

    /// Options controlling how each line is publshed to the server
    line_opts: super::options::RabbitMessageOptions,
}

pub struct AmqpRsPublisher {
    channel: Channel,
    exchange: String,
    routing_key: String,
    line_opts: RabbitMessageOptions,
}


impl AmqpRsExchange {
    /// Create a new `RabbitExchnage` endpoint that will write to the given exchnage.
    pub fn new(
        client_cert: &str,
        client_private_key: &str,
        root_ca_cert: &str,
        domain: &str,
        exchange: &str,
        line_opts: super::options::RabbitMessageOptions,
    ) -> Self {
        let handle = tokio::runtime::Handle::current();
        handle.enter();
        Self {
            connection: futures::executor::block_on(
            async {

                let args = OpenConnectionArguments::new("localhost", 5671, "", "")
                    .connection_name("test test")
                    .credentials(SecurityCredentials::new_external())
                    .tls_adaptor(
                        TlsAdaptor::with_client_auth(
                            Some(root_ca_cert.as_ref()),
                            client_cert.as_ref(),
                            client_private_key.as_ref(),
                            domain.to_owned(),
                        )
                            .unwrap(),
                    )
                    .finish();

                ////////////////////////////////////////////////////////////////
                // everything below should be the same as regular connection
                // open a connection to RabbitMQ server
                let connection = Connection::open(&args).await.unwrap();
                connection
                    .register_callback(DefaultConnectionCallback)
                    .await
                    .unwrap();
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

    fn from_command_line(args: &crate::cli::Args) -> Self {
        Self::new(&args.tls_options.cert.as_ref().unwrap(),
                  &args.tls_options.key.as_ref().unwrap(),
                  &args.tls_options.ca_cert.as_ref().unwrap(),
                  "anise",
                  &args.exchange,
                  args.rabbit_options.clone())
    }

    async fn open(&self, path: &Path, _flags: u32) -> Result<Self::Publisher, WriteError> {
        let bad_name_err =  std::io::ErrorKind::InvalidInput;

        let channel = self.connection.open_channel(None).await?;
        let routing_key = path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .file_name()
            .ok_or_else(|| WriteError::from(bad_name_err))?
            .to_str()
            .ok_or_else(|| WriteError::from(bad_name_err))?;
        Ok(
            AmqpRsPublisher{channel,
                            exchange:self.exchange.to_owned(),
                            routing_key:routing_key.to_owned(),
                            line_opts:self.line_opts.clone(),
            }
        )
    }
}

impl AmqpHeaders<'_> for amqp_serde::types::FieldTable {
    fn insert_bytes(&mut self, key:&str, bytes: &[u8]) {
        let val : amqp_serde::types::ByteArray = bytes.to_vec().try_into().unwrap();
        self.insert(key.try_into().unwrap(), amqp_serde::types::FieldValue::x(val));
    }
}

#[async_trait]
impl Publisher for AmqpRsPublisher {
    async fn wait_for_confirms(&self) -> Result<(), WriteError> {
        Ok(())
    }

    async fn basic_publish(&self, line: &[u8], sync:bool) -> Result< usize, WriteError> {
        let message = Message::new(line, &self.line_opts);
        let headers : amqp_serde::types::FieldTable = match message.headers() {
            Ok(headers) => headers,
            Err(ParsingError(err)) => {
                return Err(WriteError::ParsingError(ParsingError(err)));
            }
        };

        let props = BasicProperties::default()
            .with_content_type("utf")
            .with_headers(headers)
            .finish();
        let publish = self.channel.basic_publish(props,
                                   message.body().to_vec(),
                                   BasicPublishArguments { exchange:
                                                           self.exchange.to_owned(),
                                                           routing_key: self.routing_key.to_owned(),
                                                           mandatory: true,
                                                           immediate: false
                                   }

        ).await;
        match publish {
            Ok(_) => Ok(line.len()),
            Err(_) => Err(std::io::ErrorKind::Other.into())
        }
    }
}

impl From<amqprs::error::Error> for WriteError {
    fn from(err: amqprs::error::Error) -> WriteError {
        WriteError::NewRabbitError(err, 0)
    }
}
