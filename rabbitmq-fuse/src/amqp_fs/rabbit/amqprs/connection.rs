//! Functions for managing AMQPrs rabbit connections

use amqprs::callbacks::DefaultConnectionCallback;
use amqprs::connection::OpenConnectionArguments;
use amqprs::security::SecurityCredentials;
use amqprs::tls::TlsAdaptor;
use deadpool::managed::{Manager, RecycleError};
use deadpool::{
    async_trait,
    managed::{self, Metrics},
};

use amqprs::connection::Connection;
use miette::Result;

#[allow(unused_imports)]
use tracing::{error, info};

/// Factory to make new RabbitMQ connections
pub(crate) struct Opener {
    /// URL of the Rabbit server
    rabbit_addr: url::Url,

    /// Creditions for AMQP authentication
    credentials: SecurityCredentials,

    /// Adaptor to form TLS connection
    tls: TlsAdaptor,
}

impl Opener {
    /// Make a new `Opener` from a URL and secruity creditials
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

    /// Get a new RabbitMQ connection
    async fn get_connection(&self) -> Result<Connection, amqprs::error::Error> {
        tracing::info!("Opening new connection to {}", self.rabbit_addr);
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
        tracing::debug!("args configured");

        ////////////////////////////////////////////////////////////////
        // everything below should be the same as regular connection
        // open a connection to RabbitMQ server
        let connection = Connection::open(&args).await?;
        tracing::debug!("post open");
        loop {
            //  If returns Err, user can try again until registration succeed. the docs say
            tracing::debug!("Trying to open connection");
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

    async fn recycle(
        &self,
        conn: &mut Connection,
        _metrics: &Metrics,
    ) -> Result<(), RecycleError<Self::Error>> {
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
