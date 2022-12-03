//! Wrapper that exposes [table::DirectoryTable] and [descriptor::FileHandle] as  Fuse filesytem

pub mod dir_iter;
pub mod table;
pub(crate) mod connection;
mod buffer;
mod message;
pub mod options;
mod filesystem;
pub mod descriptor;
pub mod dir_entry;

pub(crate) use filesystem::Filesystem;

/// Inode number
pub type Ino = u64;

/// File name
pub type FileName = String;
