//! Command line parser

use clap::Parser;
use std::path::PathBuf;

use crate::amqp_fs;

#[derive(Clone, Debug, clap::Args)]
pub(crate) struct TlsArgs {
    /// P12 formatted key
    #[clap(short, long)]
    pub(crate) key: String,

    /// PEM formatted certificate chain
    #[clap(short, long)]
    pub(crate) cert: String,

    /// Password for key, if encrypted
    #[clap(long)]
    pub(crate) password: Option<String>,


}

/// Fuse filesytem that publishes to a RabbitMQ server
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Directory the filesystem will be mounted to
    pub(crate) mountpoint: PathBuf,

    /// URL of the rabbitmq server
    #[clap(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f?auth_mechanism=external"))]
    pub(crate) rabbit_addr: String,

    /// Exchange to bind directories in the mount point to
    #[clap(short, long, default_value_t = String::from(""))]
    pub(crate) exchange: String,

    /// Options to control TLS connections
    #[clap(flatten)]
    pub(crate) tls_options: TlsArgs,

    /// Options controlling the behavior of `write(2)`
    #[clap(flatten)]
    pub(crate) options: amqp_fs::options::WriteOptions,

    /// Maximum number of bytes to buffer in open files
    #[clap(short, long, default_value_t = 16777216)]
    pub(crate) buffer_size: usize,

    #[clap(long)]
    pub(crate) debug: bool
}
