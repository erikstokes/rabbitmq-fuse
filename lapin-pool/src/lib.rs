//! Helps you make a [`lapin::Connection`] using TLS and EXTERNAL
//! authentication.
//! To make a new connection, use [`ConnectionBuilder`]:
//!
//! # Examples
//! Create an opener using PLAIN authentication
//! ```rust
//! # fn main() -> miette::Result<()>{
//! use lapin_pool::ConnectionBuilder;
//! let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
//!     .plain_auth("guest")
//!     .with_password("guest")
//!     .opener()?;
//! # Ok(())}
//! ```
//!
//! Create an opener using EXTERNAL authentication
//! ```rust
//! # use miette::{NamedSource, Result};
//! # fn main() -> Result<()>{
//! # use lapin_pool::ConnectionBuilder;
//! let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
//!    .external_auth()
//!    .with_ca_pem("../test_all/tls-gen.new/basic/result/ca_certificate.pem")
//!    .with_p12("../test_all//tls-gen.new/basic/client_Lynx-167726/keycert.p12")
//!    // .key_password("bunnies") // add this if the key file is encrypted
//!    .opener()?;
//! # Ok(())}
//! ```
//!
//! Once you have an [`Opener`] you can call
//! [`Opener::get_connection`] to get a new, open RabbitMQ connection.
//! Each call will return a new connection
//!
//! # Openssl
//!
//! This crate uses openssl via the
//! [`native-tls`](https://docs.rs/native-tls/0.2.11/native_tls/) crate.
//! By default it links to an existing shared library on your system. If
//! you would instead like a static binary, include the feature
//! `native-tls/vendored` on your crate
//!
//! # From the command line
//! With the feature `clap`, you can add [`ConnectionArgs`] to any
//! `clap::Parser` struct to generate the command line options needed
//! to form RabbitMQ connections, and then create an opener using
//! [`ConnectionArgs::connection_opener`].
//!
//! ```no_run
//! # #[cfg(feature="clap")] {
//! # #[tokio::main] async fn main() -> eyre::Result<()>{
//! use clap::Parser;
//!
//! #[derive(Debug, clap::Parser)]
//! /// Clap derive command-line arguments to make rabbit connections
//! struct Args {
//!     /// Generates all the options needed to create connection openers
//!     #[command(flatten)]
//!     rabbit: lapin_pool::ConnectionArgs
//!     // Any other clap configuration goes here
//! }
//!
//! let args = Args::parse();
//! let opener = args.rabbit.connection_opener()?;
//! # Ok(()) } }
//! ```
//!
//! This will generate command line help like
//!
//! ```text
//! Clap derive command-line arguments to make rabbit connections
//!
//! Usage: example [OPTIONS]
//!
//! Options:
//!       --rabbit-addr <RABBIT_ADDR>
//!           URL of the rabbitmq server [default: amqp://127.0.0.1:5672/%2f]
//!       --key <KEY>
//!           P12 formatted key
//!       --ca-cert <CA_CERT>
//!           PEM formatted CA certificate chain
//!       --password <PASSWORD>
//!           Password for key, if encrypted
//!       --amqp-auth <AMQP_AUTH>
//!           Authentication method for RabbitMQ server. If not given, the method will be taken from the URL parameters [possible values: plain, external]
//!       --amqp-password <AMQP_PASSWORD>
//!           Password for RabbitMQ server. Required if --amqp-auth is set to 'plain'
//!       --amqp-password-file <AMQP_PASSWORD_FILE>
//!           Plain text file containing the password. A single trailing newline will be removed
//!       --amqp-user <AMQP_USER>
//!           Username for RabbitMQ server. Required if --amqp-auth is set to 'plain' [default: guest]
//!   -h, --help
//!           Print help
//! ```
//! ## Feature flags
#![doc = document_features::document_features!()]
// clippy lints
#![deny(missing_docs)]
#![allow(clippy::module_name_repetitions, clippy::single_match_else)]
#![warn(
    clippy::missing_docs_in_private_items,
    clippy::self_named_module_files,
    clippy::perf,
    clippy::missing_panics_doc,
    clippy::wildcard_imports,
    clippy::enum_glob_use,
    clippy::enum_variant_names
)]
#![cfg_attr(all(doc, CHANNEL_NIGHTLY), feature(doc_auto_cfg))]

mod builder;
mod connection;
/// Connection configuration options
mod options;

#[cfg(feature = "deadpool")]
pub mod pool;

#[cfg(feature = "clap")]
/// Clap based command-line argument builder
mod cli;

pub use builder::ConnectionBuilder;
pub use connection::Error;

/// Factory to open new connection to a fixed RabbitMQ server
///
/// # Examples
/// ```rust
/// # fn main() -> miette::Result<()> {
///     use lapin_pool::Opener;
///     use lapin::{uri::AMQPUri, ConnectionProperties};
///     Opener::new("amqp://localhost:5672/".parse().unwrap(), None, ConnectionProperties::default());
/// # Ok(()) }
/// ```
pub use connection::Opener;

// Re-export lapin and  for compatability
pub use lapin;

#[cfg(feature = "deadpool")]
pub use deadpool;

#[cfg(feature = "clap")]
/// Clap derive struct for building command-lines
pub use cli::ConnectionArgs;
