//! Helps you make a [`lapin::Connection`] using TLS and EXTERNAL
//! authentication.
//! To make a new connection, use [`ConnectionBuilder`]:
//!
//! # Examples
//! Create an opener using PLAIN authentication
//!```rust
//! # fn main() -> anyhow::Result<()>{
//! use lapin_pool::ConnectionBuilder;
//! // using plain authentication
//! let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
//!     .plain_auth("rabbit")
//!     .with_password("rabbitpw")
//!     .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
//!     .opener()?;
//! # Ok(())}
//!```
//!
//! Create an opener using EXTERNAL authentication
//!```rust
//! # fn main() -> anyhow::Result<()>{
//! # use lapin_pool::ConnectionBuilder;
//! let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
//!    .external_auth()
//!    .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
//!    .with_p12("../test_all//tls-gen/basic/client_rabbit/keycert.p12")
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
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::single_match_else)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::self_named_module_files)]
#![warn(clippy::perf)]
#![deny(missing_docs)]
#![warn(clippy::missing_panics_doc)]
#![warn(clippy::wildcard_imports)]
#![cfg_attr(all(doc, CHANNEL_NIGHTLY), feature(doc_auto_cfg))]

mod builder;
mod connection;
/// Connection configuration options
mod options;

#[cfg(feature = "deadpool")]
pub mod pool;

pub use builder::ConnectionBuilder;
pub use connection::Error;
pub use connection::Opener;
