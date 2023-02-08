//! Functions for managing TLS rabbit connections
use std::io::Read;
use std::{fs::File, sync::Arc};

use deadpool::{async_trait, managed};

use lapin::{tcp::AMQPUriTcpExt, uri::AMQPUri, Connection, ConnectionProperties};
use native_tls::TlsConnector;
use tracing::info;

use crate::cli;

/// Result of returning a connection to the pool
type RecycleResult = managed::RecycleResult<lapin::Error>;
/// Error returning the connection to the pool
type RecycleError = managed::RecycleError<lapin::Error>;

/// Factory to open `RabbitMQ` connections to the given URL
pub struct Opener {
    /// URL (including host, vhost, port and query) to open connections to
    uri: AMQPUri,
    /// Properties of the opened connections
    properties: ConnectionProperties,
    #[doc(hidden)]
    /// TLS connection wrapper
    connector: Option<Arc<TlsConnector>>,
}

impl Opener {
    /// Create a new opener to the given server
    fn new(
        uri: lapin::uri::AMQPUri,
        connector: Option<Arc<TlsConnector>>,
        properties: ConnectionProperties,
    ) -> Self {
        Self {
            uri,
            properties,
            connector,
        }
    }

    /// Create an opener using the paramaters passed on the command line
    pub fn from_command_line(args: &cli::Args, properties: ConnectionProperties) -> Self {
        let mut uri: lapin::uri::AMQPUri = args.rabbit_addr.parse().unwrap();
        if let Some(method) = args.rabbit_options.amqp_auth {
            uri.query.auth_mechanism = method.into();
        }

        if let Some(lapin::auth::SASLMechanism::Plain) = uri.query.auth_mechanism {
            uri.authority.userinfo = amq_protocol_uri::AMQPUserInfo {
                // The command line parser should require these to be
                // set if the auth method is 'plain', so these unwraps
                // are safe.
                username: args.rabbit_options.amqp_user.as_ref().unwrap().clone(),
                password: args.rabbit_options.amqp_password.as_ref().unwrap().clone(),
            };
        }

        let mut tls_builder = native_tls::TlsConnector::builder();
        if let Some(key) = &args.tls_options.key {
            tls_builder.identity(identity_from_file(key, &args.tls_options.password));
        }
        if let Some(cert) = &args.tls_options.cert {
            tls_builder.add_root_certificate(ca_chain_from_file(cert));
            tls_builder.danger_accept_invalid_hostnames(true);
        }
        let connector = Arc::new(tls_builder.build().expect("tls connector"));

        Self::new(uri, Some(connector), properties)
    }

    /// Get a new AMQP connection. If there is a TLS connector given,
    /// that will be used to establish the connection, otherwise it
    /// will be unencrypted.
    async fn get_connection(&self) -> lapin::Result<Connection> {
        if let Some(connector) = self.connector.clone() {
            let connect = move |uri: &AMQPUri| {
                println!("Connecting to {:?}", uri);
                uri.clone().connect().and_then(|stream| {
                    stream.into_native_tls(
                        &connector,
                        // &builder.build().expect("tls config"),
                        &uri.authority.host,
                    )
                })
            };

            Connection::connector(self.uri.clone(), Box::new(connect), self.properties.clone())
                .await
        } else {
            Connection::connect_uri(self.uri.clone(), self.properties.clone()).await
        }
    }
}

#[async_trait]
impl managed::Manager for Opener {
    type Type = lapin::Connection;
    type Error = lapin::Error;

    async fn create(&self) -> lapin::Result<Self::Type> {
        info!("Opening new connection");
        self.get_connection().await
    }

    // copypasta from https://github.com/bikeshedder/deadpool/blob/d7167eaf47ccaadabfb831ce3718cdebe51185ba/lapin/src/lib.rs#L91
    async fn recycle(&self, conn: &mut lapin::Connection) -> RecycleResult {
        match conn.status().state() {
            lapin::ConnectionState::Connected => Ok(()),
            other_state => Err(RecycleError::Message(format!(
                "lapin connection is in state: {:?}",
                other_state
            ))),
        }
    }
}

/// Pool of `RabbitMQ` connections. Connections will be lazily
/// re-opened when closed as needed
pub(crate) type ConnectionPool = managed::Pool<Opener>;

/// Load a TLS identity from p12 formatted file path
fn identity_from_file(p12_file: &str, password: &Option<String>) -> native_tls::Identity {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert)
        .expect("unable to read cleint cert");
    match native_tls::Identity::from_pkcs12(&key_cert, password.as_ref().unwrap_or(&String::new()))
    {
        Ok(ident) => ident,
        Err(..) => {
            let password = rpassword::prompt_password("Key password: ").unwrap();
            native_tls::Identity::from_pkcs12(&key_cert, &password).expect("Unable to decrypt key")
        }
    }
}

/// Load a certificate authority from a PEM formatted file path
fn ca_chain_from_file(pem_file: &str) -> native_tls::Certificate {
    let mut f = File::open(pem_file).expect("Unable to open ca chain");
    let mut ca_chain = Vec::new();
    f.read_to_end(&mut ca_chain)
        .expect("Unable to read ca chain");
    native_tls::Certificate::from_pem(&ca_chain).expect("unable to parse certificate")
}