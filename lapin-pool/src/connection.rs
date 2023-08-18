//! Functions for managing TLS rabbit connections
use std::io::Read;
use std::{fs::File, sync::Arc};

use deadpool::{async_trait, managed};

use lapin::{tcp::AMQPUriTcpExt, uri::AMQPUri, Connection, ConnectionProperties};
use native_tls::TlsConnector;
use tracing::{error, info, warn};

use super::options::{AmqpPlainAuth, AuthMethod, TlsArgs};

/// Certificate errors
#[derive(Debug, thiserror::Error)]
enum Error {
    /// One of the certificate files failed to parse
    #[error("Failed to parse input {0}")]
    ParseError(String),

    /// Failed to read a password from the user
    #[error("Failed to read password")]
    PasswordError,

    #[error("Error in forming TLS connection")]
    TlsError(#[from] native_tls::Error),
}

/// Arguments to open a RabbitMQ connection
#[derive(clap::Args, Debug)]
pub struct RabbitCommand {
    /// URL of the rabbitmq server
    #[arg(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f"))]
    pub rabbit_addr: String,

    /// Authentication method for RabbitMQ server
    #[arg(long)]
    pub amqp_auth: Option<AuthMethod>,

    /// Username password for plain authentication
    #[command(flatten)]
    pub plain_auth: AmqpPlainAuth,
    /// Options to control TLS connections
    #[command(flatten)]
    pub tls_options: TlsArgs,
}

impl RabbitCommand {
    /// Parse the enpoint url string to a [`url::Url`]
    pub fn endpoint_url(&self) -> anyhow::Result<url::Url> {
        Ok(url::Url::parse(&self.rabbit_addr)?)
    }
}

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
    pub fn from_command_line(
        args: &RabbitCommand,
        properties: ConnectionProperties,
    ) -> anyhow::Result<Self> {
        let mut uri: lapin::uri::AMQPUri = Into::<String>::into(args.endpoint_url()?)
            .parse()
            .map_err(|s| {
                error!(url = args.rabbit_addr, "Unable to parse server URL");
                Error::ParseError(s)
            })?;
        if let Some(method) = args.amqp_auth {
            uri.query.auth_mechanism = method.into();
        }

        if let Some(lapin::auth::SASLMechanism::Plain) = uri.query.auth_mechanism {
            let user = &args.plain_auth;
            uri.authority.userinfo = user.try_into()?;
        }

        let mut tls_builder = native_tls::TlsConnector::builder();
        if let Some(key) = &args.tls_options.key {
            tls_builder.identity(
                identity_from_file(key, &args.tls_options.password)
                    .or(Err(Error::PasswordError))?,
            );
        }
        if let Some(cert) = &args.tls_options.ca_cert {
            tls_builder.add_root_certificate(ca_chain_from_file(cert));
            tls_builder.danger_accept_invalid_hostnames(true);
        }
        let connector = Arc::new(tls_builder.build()?);

        Ok(Self::new(uri, Some(connector), properties))
    }

    /// Get a new AMQP connection. If there is a TLS connector given,
    /// that will be used to establish the connection, otherwise it
    /// will be unencrypted.
    pub async fn get_connection(&self) -> lapin::Result<Connection> {
        if let Some(connector) = self.connector.clone() {
            let connect = move |uri: &AMQPUri| {
                info!("Connecting to {:?}", uri);
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
pub type ConnectionPool = managed::Pool<Opener>;

/// Load a TLS identity from p12 formatted file path
fn identity_from_file(
    p12_file: &str,
    password: &Option<String>,
) -> Result<native_tls::Identity, Error> {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert)
        .expect("unable to read cleint cert");
    match native_tls::Identity::from_pkcs12(&key_cert, password.as_ref().unwrap_or(&String::new()))
    {
        Ok(ident) => Ok(ident),
        Err(e) => {
            warn!(error=?e, p12_file=p12_file, "Failed to open key with password");
            let password =
                rpassword::prompt_password("Key password: ").map_err(|_| Error::PasswordError)?;
            Ok(native_tls::Identity::from_pkcs12(&key_cert, &password)?)
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

impl From<AuthMethod> for Option<lapin::auth::SASLMechanism> {
    fn from(val: AuthMethod) -> Option<lapin::auth::SASLMechanism> {
        Some(match val {
            AuthMethod::Plain => lapin::auth::SASLMechanism::Plain,
            AuthMethod::External => lapin::auth::SASLMechanism::External,
        })
    }
}

impl TryFrom<&AmqpPlainAuth> for amq_protocol_uri::AMQPUserInfo {
    type Error = std::io::Error;

    fn try_from(val: &AmqpPlainAuth) -> Result<amq_protocol_uri::AMQPUserInfo, Self::Error> {
        Ok(amq_protocol_uri::AMQPUserInfo {
            // The command line parser should require these to be
            // set if the auth method is 'plain', so these unwraps
            // are safe.
            username: val.amqp_user.to_string(),
            // Exactly one of password or password file is set
            password: val.password()?.unwrap_or_default(),
        })
    }
}
