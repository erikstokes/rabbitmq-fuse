//! Helps you make a [`lapin::Connection`] using TLS and EXTERNAL
//! authentication

#![allow(clippy::module_name_repetitions)]
#![allow(clippy::single_match_else)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::self_named_module_files)]
#![warn(clippy::perf)]
#![deny(missing_docs)]
#![warn(clippy::missing_panics_doc)]

mod builder;
pub mod connection;
/// Connection configuration options
mod options;

#[cfg(feature = "deadpool")]
pub mod pool;

pub use builder::ConnectionBuilder;
