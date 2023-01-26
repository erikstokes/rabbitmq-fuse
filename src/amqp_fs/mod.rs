//! Wrapper that exposes [`table::DirectoryTable`] and [`descriptor::FileHandle`] as  Fuse filesytem

mod buffer;
pub mod descriptor;
pub mod dir_entry;
pub mod dir_iter;
mod filesystem;
pub mod options;
pub mod publisher;
pub mod table;

pub(crate) use filesystem::Filesystem;
pub(crate) use filesystem::Mountable;

/// Inode number
pub type Ino = u64;

/// File name
pub type FileName = String;

pub mod rabbit;
// pub use rabbit::endpoint::RabbitExchnage;
