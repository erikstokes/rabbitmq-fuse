//! Builder for making TLS RabbitMQ connections using ['lapin`] using
//! PLAIN or EXTERNAL authenticatio

use std::marker::PhantomData;
use std::path::Path;

use crate::connection::{Opener, RabbitCommand};
use crate::options::AuthMethod;

/// Builder to make an [`Opener`], from which you
/// can make a [`lapin::Connection`]
pub struct ConnectionBuilder<Auth: AuthType> {
    #[doc(hidden)]
    command: RabbitCommand,
    #[doc(hidden)]
    properties: lapin::ConnectionProperties,
    #[doc(hidden)]
    _marker: PhantomData<Auth>,
}

/// Marker trait for [`ConnectionBuilder`] typestate. You can't make
/// this yourself
#[doc(hidden)]
pub trait AuthType: private::Sealed {}

impl AuthType for auth::Plain {}
impl AuthType for auth::External {}
impl AuthType for auth::None {}

#[doc(hidden)]
mod private {
    pub trait Sealed {}
}

impl private::Sealed for auth::None {}
impl private::Sealed for auth::Plain {}
impl private::Sealed for auth::External {}

#[doc(hidden)]
pub mod auth {
    pub struct None;
    pub struct Plain;
    pub struct External;
}

impl<Auth: AuthType> ConnectionBuilder<Auth> {
    /// Start building a new RabbitMQ connection to the given URL.
    /// Connection parameters can be given as a query string in the
    /// URL, but parameters given in the builder will override those.
    pub fn new(url: &str) -> Self {
        Self {
            command: RabbitCommand::new(url),
            properties: Default::default(),
            _marker: PhantomData,
        }
    }

    /// Use the given [`lapin::ConnectionProperties`]
    pub fn with_properties(mut self, properties: lapin::ConnectionProperties) -> Self {
        self.properties = properties;
        self
    }

    /// Verify the connection using the given CA certificate file, in
    /// PEM format.
    pub fn with_ca_pem(mut self, ca_cert: &str) -> Self {
        self.command.tls_options.ca_cert = Some(ca_cert.to_string());
        self
    }

    /// Use the p12 formatted key/cert file to authorize yourself to
    /// the server
    pub fn with_p12(mut self, key: &str) -> Self {
        self.command.tls_options.key = Some(key.to_string());
        self
    }

    /// Return the configured [`crate::Opener`]
    pub fn opener(self) -> anyhow::Result<Opener> {
        Opener::from_command_line(&self.command, self.properties)
    }

    #[cfg(feature = "deadpool")]
    pub fn pool(self) -> anyhow::Result<deadpool::managed::PoolBuilder<Opener>> {
        let opener = self.opener()?;
        Ok(crate::pool::ConnectionPool::builder(opener))
    }
}

impl ConnectionBuilder<auth::None> {
    /// Use EXTERNAL auth. If you call this, you essentially have to
    /// call [`ConnectionBuilder::with_p12`]
    pub fn external_auth(mut self) -> ConnectionBuilder<auth::External> {
        self.command.amqp_auth = Some(AuthMethod::External);
        ConnectionBuilder {
            command: self.command,
            properties: self.properties,
            _marker: PhantomData,
        }
    }

    /// Use PLAIN (username/password) authentication
    pub fn plain_auth(mut self, user: &str) -> ConnectionBuilder<auth::Plain> {
        let auth = crate::options::AmqpPlainAuth {
            amqp_user: user.to_string(),
            ..Default::default()
        };
        self.command.amqp_auth = Some(AuthMethod::Plain(auth));
        ConnectionBuilder {
            command: self.command,
            properties: self.properties,
            _marker: PhantomData,
        }
    }
}

impl ConnectionBuilder<auth::Plain> {
    /// Give the password as a plain text string
    pub fn with_password(mut self, password: &str) -> Self {
        if let Some(AuthMethod::Plain(ref mut plain)) = self.command.amqp_auth {
            plain.amqp_password = Some(password.to_string())
        } else {
            unreachable!();
        }
        self
    }

    /// Read the password from the given file
    pub fn with_password_file(mut self, password_file: impl AsRef<Path>) -> Self {
        if let Some(AuthMethod::Plain(ref mut plain)) = self.command.amqp_auth {
            plain.amqp_password_file = Some(password_file.as_ref().to_owned())
        } else {
            unreachable!();
        }
        self
    }
}
