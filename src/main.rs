//! Fuse filesytem mount that publishes to a RabbitMQ server
//!
//! Usage:
//! ```
//! rabbitmq-fuse --exchange="exchange" --rabbit-addr="amqp://rabbit.host/" mountpoint/
//! ```
//!
//! Creates a one level deep filesystem. Directories
//! correspond to routing keys and files are essentially meaningless.
//! Each line written to `dirctory/file` is converted to a RabbitMQ
//! `basic_publish` with a `routing_key` of "directory" and an
//! exchange specified at mount time
//!
//! Confirmations are enabled and failed publishes will send an error
//! back after calling `fsync(2)` or `fclose(2)`.
//!
//! Publishing and writing are done asynchronously unless the file is
//! opened with O_SYNC, in which case, writes become blocking and very
//! slow.
//!
//! As is the normal case with  `write(2)`, the number of bytes stored
//! in  the  filesytems internal  buffers  will  be returned,  with  0
//! indicating  an error  and  setting `errno`.  Successful writes  do
//! *not* mean the data was published to the server, only that it will
//! be  published.  Only  complete   lines  (separated  by  '\n')  are
//! published.  By default, incomplete   lines  are  not  published,   even  after
//! `fsync(2)`, but will be published when the file handle is released
//! (that is, when the last holder of the descriptor releasees it). This behavior can be modified via [amqp_fs::options::LinePublishOptions::fsync]
//!
//! All files have size 0 and appear empty, even after writes.
//! Directories may not contain subdirectories and the mount point can
//! contain no regular files. Only regular files and directories are
//! supported.

#![warn(clippy::all)]

use anyhow::Result;
use std::sync::Arc;

use polyfuse::KernelConfig;

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};

use clap::Parser;

mod amqp_fs;
mod cli;
mod session;

use crate::amqp_fs::publisher::Endpoint;
use crate::amqp_fs::rabbit::endpoint::RabbitExchnage;
use crate::amqp_fs::Filesystem;


/// Main command line entry point
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::FmtSubscriber::builder()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // let mut args = pico_args::Arguments::from_env();
    let args = cli::Args::parse();
    debug!("Got command line arguments {:?}", args);

    // let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    if !args.mountpoint.is_dir() {
        eprintln!("mountpoint must be a directory");
        std::process::exit(1);
    }

    info!(
        "Mounting RabbitMQ server {host} at {mount}/",
        mount = &args.mountpoint.display(),
        host = args.rabbit_addr
    );

    let mut fuse_conf = KernelConfig::default();
    fuse_conf.export_support(false);
    let session = session::AsyncSession::mount(args.mountpoint.clone(), fuse_conf).await?;

    let fs: Arc<dyn amqp_fs::Mountable+Send+Sync> = if args.debug {
        let endpoint = amqp_fs::publisher::StdOut::from_command_line(&args);
        Arc::new(Filesystem::new(endpoint, args.options))
    } else {
        let endpoint = RabbitExchnage::from_command_line(&args);
        Arc::new(Filesystem::new(endpoint, args.options))
    };

    let for_ctrlc = fs.clone();
    ctrlc::set_handler( move || {
        // for_ctrlc.store(true, Ordering::Relaxed);
        for_ctrlc.stop();
    })
    .expect("Setting signal handler");

    fs.run(session).await?;

    info!("Shutting down");

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     extern crate test;
//     use super::*;
//     use test::Bencher;

//     #[bench]
//     fn rabbit_write_lines(bencher: &mut Bencher) -> Result<()> {
//          let args = cli::Args::parse();

//         let endpoint = amqp_fs::publisher::rabbit::RabbitExchnage::from_command_line(&args);
//         let fs = Arc::new(amqp_fs::Filesystem::new(endpoint, &args));

//         Ok(())
//     }
// }
