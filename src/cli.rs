//! Command line parser

use clap::Parser;
use std::path::PathBuf;

use crate::amqp_fs;

/// Options controlling TLS connections and certificate based
/// authentication
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct TlsArgs {
    /// P12 formatted key
    #[clap(short, long)]
    pub(crate) key: Option<String>,

    /// PEM formatted certificate chain
    #[clap(short, long)]
    pub(crate) cert: Option<String>,

    #[clap(long)]
    pub(crate) ca_cert: Option<String>,

    /// Password for key, if encrypted
    #[clap(long)]
    pub(crate) password: Option<String>,
}

/// Options controlling the interaction with the kernel
#[derive(Clone, Debug, clap::Args)]
pub(crate) struct FuseOptions {
    /// The maximum number of fuse background tasks to allow at once
    #[clap(long, default_value_t = 10)]
    pub max_fuse_requests: u16,

    /// The maximum size of the write buffer passed from the kernel
    #[clap(long, default_value_t = 0x400000)]
    pub fuse_write_buffer: u32,
}

/// Fuse filesytem that publishes to a `RabbitMQ` server
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Directory the filesystem will be mounted to
    pub(crate) mountpoint: PathBuf,

    /// URL of the rabbitmq server
    #[clap(short, long, default_value_t = String::from("amqp://127.0.0.1:5671/%2f"))]
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

    /// Options controlling the interaction with FUSE
    #[clap(flatten)]
    pub(crate) fuse_opts: FuseOptions,

    /// Options for the RabbitMQ endpoint
    #[clap(flatten)]
    pub(crate) rabbit_options: crate::amqp_fs::rabbit::options::RabbitMessageOptions,

    /// Maximum number of bytes to buffer in open files
    #[clap(short, long, default_value_t = 16777216)]
    pub(crate) buffer_size: usize,

    /// Run the mount in debug mode where writes go to stdout
    #[clap(long)]
    pub(crate) debug: bool,

    /// Background the process after startup
    #[clap(long)]
    pub(crate) daemon: bool,

    /// File to write logs to. Will log to stderr if not given
    #[clap(long)]
    pub(crate) logfile: Option<PathBuf>
}
