//! Functions for managing TLS rabbit connections
use std::io::Read;
use std::{fs::File, sync::Arc};

use lapin::{tcp::AMQPUriTcpExt, uri::AMQPUri, Connection, ConnectionProperties};
use native_tls::TlsConnector;
use tracing::{debug, error, info, instrument, warn};

use miette::Diagnostic;

use super::options::{AmqpPlainAuth, AuthMethod, TlsArgs};

/// Certificate errors
#[derive(Diagnostic, Debug, thiserror::Error)]
#[diagnostic(help("Failed to form requested connection"))]
pub enum Error {
    /// One of the certificate files failed to parse
    #[error("Failed to parse input {0}")]
    #[diagnostic(code(connection::certs))]
    Parse(String),

    /// Failed to read a password from the user
    #[error("Failed to read password")]
    #[diagnostic(code(connection::password))]
    Password,

    /// Errors coming from TLS, for example malformed certificates
    #[error("Error in forming TLS connection")]
    #[diagnostic(code(connection::tls))]
    Tls(#[from] native_tls::Error),

    /// Failure to read P12 key file
    #[error("Error reading P12 key file {file}. Bad password?")]
    #[diagnostic(code(connection::certificate::read), url(docsrs))]
    P12 {
        /// Path of file that failed to load
        file: String,

        /// Original TLS library error
        #[source]
        source: native_tls::Error,
    },

    /// IO Error
    #[error("IO error")]
    #[diagnostic()]
    IO(#[from] std::io::Error),

    /// IO Error with the path of the file that caused the error
    #[error("IO Error reading '{path}'")]
    FilePathError {
        /// Original IO Error
        #[source]
        source: std::io::Error,
        /// FIle path
        path: String,
    },
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
    pub(crate) tls_options: Option<TlsArgs>,

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

/// Factory to open `RabbitMQ` connections to the given URL. This type
/// has no public constructors, to make one, use
/// [`crate::ConnectionBuilder`]
pub struct Opener {
    /// URL (including host, vhost, port and query) to open connections to
    uri: AMQPUri,
    /// Properties of the opened connections
    properties: ConnectionProperties,
    #[doc(hidden)]
    /// TLS connection wrapper
    connector: Option<Arc<TlsConnector>>,
}

impl std::fmt::Debug for Opener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opener")
            .field("uri", &self.uri)
            .field("connector", &self.connector)
            .finish_non_exhaustive()
    }
}

impl Opener {
    /// Create a new opener to the given server
    pub fn new(
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

    /// Create an [`Opener`] using the paramaters packaged in `args`.
    /// New connections will be opened using the connection properties
    /// in `properties`
    #[instrument(skip(properties))]
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

        let tls_options = match &args.tls_options {
            Some(tls_options) => tls_options,
            None => return Ok(Self::new(uri, None, properties)),
        };

        let mut tls_builder = native_tls::TlsConnector::builder();
        if let Some(key) = &tls_options.key {
            info!(key = &key, "Loading identity");
            tls_builder.identity(
                identity_from_file(key, &tls_options.password, args.prompt).map_err(
                    |e| match e {
                        Error::Tls(e) => Error::P12 {
                            file: key.clone(),
                            source: e,
                        },
                        _ => e,
                    },
                )?,
            );
        }
        if let Some(cert) = &tls_options.ca_cert {
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
        debug!("Creating new connection");
        if let Some(connector) = self.connector.clone() {
            debug!("Creating TLS connection");
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
    let mut f = File::open(p12_file).map_err(|err| Error::FilePathError {
        source: err,
        path: p12_file.to_string(),
    })?;
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

#[cfg(test)]
mod test {
    use super::*;

    const RABBIT_URL: Option<&str> = std::option_env!("RABBIT_URL");
    const DEFAULT_URL: &str = "amqp://localhost:5672/%2f";

    fn get_url() -> AMQPUri {
        RABBIT_URL
            .unwrap_or(DEFAULT_URL)
            .parse()
            .unwrap_or_else(|_| panic!("Can't parse URL string to {:?}. Set the environment variable RABBIT_URL to configure the test server", RABBIT_URL))
    }

    #[test]
    fn cmd_from_url() -> eyre::Result<()> {
        let url_str = "amqp://127.0.0.1:5672/%2f";
        let cmd = RabbitCommand::new(url_str);
        let url = cmd.endpoint_url()?;
        assert_eq!(url.as_str(), url_str);
        Ok(())
    }

    #[test]
    fn make_opener() {
        let url = get_url();
        Opener::new(url, None, ConnectionProperties::default());
    }

    #[tokio::test]
    #[ignore = "needs server"]
    async fn make_connection() {
        let url = get_url();
        let opener = Opener::new(url, None, ConnectionProperties::default());
        let _conn = opener.get_connection().await.unwrap();
    }
}
