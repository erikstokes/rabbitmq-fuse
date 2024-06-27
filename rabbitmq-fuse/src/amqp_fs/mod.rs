//! Wrapper that exposes [`table::DirectoryTable`] and [`descriptor::FileHandle`] as  Fuse filesytem

mod buffer;
pub mod descriptor;
pub mod dir_entry;
pub mod dir_iter;

/// Filesystem handle. Provides the shim between the fuse
/// [`crate::session::AsyncSession`] and the directory table and
/// publisher. Each method is called to handle a specific syscall
mod filesystem;
pub mod options;

/// Trait that publishes lines to a given endpoint and allows
/// blocking/errors to for publication confirmations
pub mod publisher;
pub mod table;

pub(crate) use filesystem::Filesystem;
pub(crate) use filesystem::Mountable;

/// Inode number
pub type Ino = u64;

/// File name
pub type FileName = String;

/// `RabbitMQ` Endpoint
pub mod rabbit;
// pub use rabbit::endpoint::RabbitExchnage;

/// Kafka endpoint implementation
pub mod kafka;
