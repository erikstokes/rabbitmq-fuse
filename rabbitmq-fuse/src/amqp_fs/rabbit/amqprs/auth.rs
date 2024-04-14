/// Server authentication method
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum AuthMethod {
    /// Plain username/password authentication
    Plain,
    /// External certificate based authentication
    External,
}

/// Username/password data for AMQP PLAIN auth method
#[derive(clap::Args, Clone, Debug, Default)]
pub struct AmqpPlainAuth {
    /// Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[arg(long)]
    amqp_password: Option<String>,

    /// Plain text file containing the password. A single trailing newline will be removed
    #[arg(long, conflicts_with = "amqp_password")]
    amqp_password_file: Option<std::path::PathBuf>,

    /// Username for RabbitMQ server. Required if --amqp-auth is set to 'plain'
    #[arg(long, default_value = "guest")]
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