//! Command line parser

use clap::Parser;
use std::{path::PathBuf, sync::Arc};

use crate::amqp_fs::{self, rabbit::RabbitCommand, options::WriteOptions};
use crate::amqp_fs::publisher::Endpoint;


// #[enum_dispatch]
#[derive(clap::Subcommand, Debug)]
pub enum Endpoints {
    /// RabbitMQ endpoint that publishes to a fixed exchange
    Rabbit(RabbitCommand),
    /// Endpoint that publishes to a file (for debugging)
    Stream(amqp_fs::publisher::StreamCommand),
}

// #[enum_dispatch(Endpoints)]
pub(crate) trait EndpointCommand
{
    type Endpoint: amqp_fs::publisher::Endpoint<Options = Self> + 'static;

    /// Get the filesystem mount for the corresponding endpoint
    fn get_mount(&self, write: &WriteOptions) ->  anyhow::Result<Arc<dyn amqp_fs::Mountable + Send + Sync + 'static>> {
        let ep = Self::Endpoint::from_command_line(self)?;
        Ok(Arc::new(amqp_fs::Filesystem::new(ep, write.clone())))
    }
}


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

    /// PEM formatted CA certificate chain
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

    /// Options controlling the behavior of `write(2)`
    #[clap(flatten)]
    pub(crate) options: amqp_fs::options::WriteOptions,

    /// Options controlling the interaction with FUSE
    #[clap(flatten)]
    pub(crate) fuse_opts: FuseOptions,

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
    pub(crate) logfile: Option<PathBuf>,

    /// Endpoint to publish to
    #[command(subcommand)]
    pub(crate) endpoint: Endpoints,
}
