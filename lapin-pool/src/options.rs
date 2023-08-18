/// Options controlling TLS connections and certificate based
/// authentication
#[derive(Clone, Debug, Default)]
pub(crate) struct TlsArgs {
    /// P12 formatted key
    pub key: Option<String>,

    /// PEM formatted certificate chain
    pub cert: Option<String>,

    /// PEM formatted CA certificate chain
    pub ca_cert: Option<String>,

    /// Password for key, if encrypted
    pub password: Option<String>,
}

/// Server authentication method
#[derive(Clone, Debug)]
pub(crate) enum AuthMethod {
    /// Plain username/password authentication
    Plain(AmqpPlainAuth),
    /// External certificate based authentication
    External,
}

/// Username/password data for AMQP PLAIN auth method
#[derive(Clone, Debug, Default)]
pub(crate) struct AmqpPlainAuth {
    /// Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    pub amqp_password: Option<String>,

    /// Plain text file containing the password. A single trailing newline will be removed
    pub amqp_password_file: Option<std::path::PathBuf>,

    /// Username for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    pub amqp_user: String,
}

impl AmqpPlainAuth {
    /// Return the password for PLAIN auth, or None if no password is
    /// given. Returns an io error if the password file is given but
    /// can't be read
    pub fn password(&self) -> std::io::Result<Option<String>> {
        let pass = if let Some(pfile) = &self.amqp_password_file {
            let p = std::fs::read_to_string(pfile)?;
            match p.strip_suffix('\n') {
                Some(p) => Some(p.to_string()),
                None => Some(p.to_string()),
            }
        } else {
            self.amqp_password.clone()
        };
        Ok(pass)
    }
}
