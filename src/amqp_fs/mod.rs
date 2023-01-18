//! Wrapper that exposes [`table::DirectoryTable`] and [`descriptor::FileHandle`] as  Fuse filesytem

pub mod dir_iter;
pub mod table;
mod buffer;
pub mod options;
mod filesystem;
pub mod descriptor;
pub mod dir_entry;
pub mod publisher;

pub(crate) use filesystem::Filesystem;
pub(crate) use filesystem::Mountable;

/// Inode number
pub type Ino = u64;

/// File name
pub type FileName = String;

pub mod rabbit;
// pub use rabbit::endpoint::RabbitExchnage;
