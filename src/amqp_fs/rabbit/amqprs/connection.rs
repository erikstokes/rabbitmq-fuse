//! Functions for managing AMQPrs rabbit connections

use amqprs::callbacks::DefaultConnectionCallback;
use amqprs::connection::OpenConnectionArguments;
use amqprs::security::SecurityCredentials;
use amqprs::tls::TlsAdaptor;
use deadpool::managed::{Manager, RecycleError};
use deadpool::{async_trait, managed};

use amqprs::connection::Connection;
use anyhow::Result;

#[allow(unused_imports)]
use tracing::{error, info};

pub(super) struct Opener {
    /// URL of the Rabbit server
    rabbit_addr: url::Url,

    /// Creditions for AMQP authentication
    credentials: SecurityCredentials,

    /// Adaptor to form TLS connection
    tls: TlsAdaptor,
}

impl Opener {
    /// Create a new `RabbitExchnage` endpoint that will write to the
    /// given exchnage. All certificate files must be in PEM form.
    pub fn new(
        // client_cert: &str,
        // client_private_key: &str,
        // root_ca_cert: &str,
        rabbit_addr: &url::Url,
        credentials: SecurityCredentials,
        tls: TlsAdaptor,
    ) -> Self {
        Self {
            rabbit_addr: rabbit_addr.clone(),
            credentials,
            tls,
        }
    }

    async fn get_connection(&self) -> Result<Connection, amqprs::error::Error> {
        let args = OpenConnectionArguments::new(
            self.rabbit_addr.host_str().expect("No host name provided"),
            self.rabbit_addr.port().unwrap_or(5671),
            "",
            "",
        )
        .connection_name("test test")
        .credentials(self.credentials.clone())
        .tls_adaptor(self.tls.clone())
        .finish();

        ////////////////////////////////////////////////////////////////
        // everything below should be the same as regular connection
        // open a connection to RabbitMQ server
        let connection = Connection::open(&args).await?;
        loop {
            //  If returns Err, user can try again until registration succeed. the docs say
            if let Ok(()) = connection
                .register_callback(DefaultConnectionCallback)
                .await
            {
                break;
            }
        }
        Ok(connection)
    }
}

#[async_trait]
impl Manager for Opener {
    type Type = Connection;
    type Error = amqprs::error::Error;

    async fn create(&self) -> Result<Connection, Self::Error> {
        info!("Opening new connection");
        Ok(self.get_connection().await?)
    }

    async fn recycle(&self, conn: &mut Connection) -> Result<(), RecycleError<Self::Error>> {
        if conn.is_open() {
            Ok(())
        } else {
            Err(managed::RecycleError::Message(
                "amqprs connection isn't open".to_string(),
            ))
        }
    }
}

/// Pool of `RabbitMQ` connections. Connections will be lazily
/// re-opened when closed as needed
pub(super) type ConnectionPool = managed::Pool<Opener>;
