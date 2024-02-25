//! Helps you make a [`lapin::Connection`] using TLS and EXTERNAL
//! authentication.
//! To make a new connection, use [`ConnectionBuilder`]:
//!
//! # Examples
//! Create an opener using PLAIN authentication
//!```rust
//! # fn main() -> miette::Result<()>{
//! use lapin_pool::ConnectionBuilder;
//! let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
//!     .plain_auth("guest")
//!     .with_password("guest")
//!     .opener()?;
//! # Ok(())}
//!```
//!
//! Create an opener using EXTERNAL authentication
//!```rust
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
//!```
//!
//! Once you have an [`Opener`] you can call
//! [`Opener::get_connection`] to get a new, open RabbitMQ connection.
//! Each call will return a new connection
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
