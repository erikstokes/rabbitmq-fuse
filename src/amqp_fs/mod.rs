//! Wrapper that exposes [table::DirectoryTable] and [descriptor::FileHandle] as  Fuse filesytem

pub mod dir_iter;
pub mod table;
mod connection;
mod buffer;
mod message;
pub mod options;
mod filesystem;
pub mod descriptor;

pub(crate) use filesystem::Rabbit;
