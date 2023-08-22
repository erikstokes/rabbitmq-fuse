//! Functions for managing TLS rabbit connections
use std::io::Read;
use std::{fs::File, sync::Arc};

use lapin::{tcp::AMQPUriTcpExt, uri::AMQPUri, Connection, ConnectionProperties};
use native_tls::TlsConnector;
use tracing::{error, info, warn};

use super::options::{AmqpPlainAuth, AuthMethod, TlsArgs};

/// Certificate errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// One of the certificate files failed to parse
    #[error("Failed to parse input {0}")]
    Parse(String),

    /// Failed to read a password from the user
    #[error("Failed to read password")]
    Password,

    /// Errors coming from TLS, for example malformed certificates
    #[error("Error in forming TLS connection")]
    Tls(#[from] native_tls::Error),

    /// Failure to read P12 key file
    #[error("Error reading P12 key file {file}. Bad password?")]
    P12 {
        /// Path of file that failed to load
        file: String,
        /// Original TLS library error
        #[source]
        source: native_tls::Error,
    },

    /// IO Error
    #[error("IO error")]
    IO(#[from] std::io::Error),
}

/// Result type that returns an [`Error`]
type Result<T> = std::result::Result<T, Error>;

/// Arguments to open a RabbitMQ connection
#[derive(Debug, Default)]
pub(crate) struct RabbitCommand {
    /// URL of the rabbitmq server
    rabbit_addr: String,

    /// Authentication method for RabbitMQ server
    pub(crate) amqp_auth: Option<AuthMethod>,

    /// Options to control TLS connections
    pub(crate) tls_options: TlsArgs,

    /// Prompt for missing passwords if the P12 key is encrypted
    pub(crate) prompt: bool,
}

impl RabbitCommand {
    /// Create a new command to open connections to the given URL
    pub(crate) fn new(url: &str) -> Self {
        Self {
            rabbit_addr: url.to_string(),
            prompt: false,
            ..Default::default()
        }
    }

    /// Parse the enpoint url string to a [`url::Url`]
    pub fn endpoint_url(&self) -> Result<url::Url> {
        url::Url::parse(&self.rabbit_addr).or(Err(Error::Parse(self.rabbit_addr.to_string())))
    }
}

/// Factory to open `RabbitMQ` connections to the given URL. To make this, use [`crate::ConnectionBuilder`]
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
    pub(crate) fn from_command_line(
        args: &RabbitCommand,
        properties: ConnectionProperties,
    ) -> Result<Self> {
        let mut uri: lapin::uri::AMQPUri = Into::<String>::into(args.endpoint_url()?)
            .parse()
            .map_err(|s| {
                error!(url = args.rabbit_addr, "Unable to parse server URL");
                Error::Parse(s)
            })?;
        if let Some(method) = &args.amqp_auth {
            uri.query.auth_mechanism = method.clone().into();
        }

        if let Some(lapin::auth::SASLMechanism::Plain) = uri.query.auth_mechanism {
            if let Some(AuthMethod::Plain(ref user)) = &args.amqp_auth {
                uri.authority.userinfo = user.try_into()?;
            } else {
                tracing::warn!(
                    url = ?uri,
                    "URL query parameters do not match arguments. Arguments take precedence"
                );
            }
        }

        let mut tls_builder = native_tls::TlsConnector::builder();
        if let Some(key) = &args.tls_options.key {
            tls_builder.identity(
                identity_from_file(key, &args.tls_options.password, args.prompt).map_err(|e| {
                    match e {
                        Error::Tls(e) => Error::P12 {
                            file: key.clone(),
                            source: e,
                        },
                        _ => e,
                    }
                })?,
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

/// Load a TLS identity from p12 formatted file path. Will attempt to
/// read the key file using the given password. If that fails (for
/// example, it is `None`) then optionally prompt the user for a
/// password based on the value of `prompt_on_error`.
fn identity_from_file(
    p12_file: &str,
    password: &Option<String>,
    prompt_on_error: bool,
) -> Result<native_tls::Identity> {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert)
        .expect("unable to read cleint cert");
    match native_tls::Identity::from_pkcs12(&key_cert, password.as_ref().unwrap_or(&String::new()))
    {
        Ok(ident) => Ok(ident),
        Err(e) => {
            if !prompt_on_error {
                return Err(e.into());
            }
            warn!(error=?e, p12_file=p12_file, "Failed to open key with password");
            let password =
                rpassword::prompt_password("Key password: ").map_err(|_| Error::Password)?;
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
            AuthMethod::Plain(_) => lapin::auth::SASLMechanism::Plain,
            AuthMethod::External => lapin::auth::SASLMechanism::External,
        })
    }
}

impl TryFrom<&AmqpPlainAuth> for amq_protocol_uri::AMQPUserInfo {
    type Error = std::io::Error;

    fn try_from(
        val: &AmqpPlainAuth,
    ) -> std::result::Result<amq_protocol_uri::AMQPUserInfo, Self::Error> {
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
