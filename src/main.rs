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
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::task::{self, JoinHandle};

use polyfuse::{KernelConfig, Operation};

#[allow(unused_imports)]
use tracing::{debug, error, info, Level};

use clap::Parser;

mod amqp_fs;
mod cli;
mod session;

use crate::amqp_fs::publisher::Endpoint;
use crate::amqp_fs::rabbit::endpoint::RabbitExchnage;

/// Main command line entry point
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::FmtSubscriber::builder()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // let mut args = pico_args::Arguments::from_env();
    let args = cli::Args::parse();

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

    let endpoint = RabbitExchnage::from_command_line(&args);

    let fs = Arc::new(amqp_fs::Filesystem::new(endpoint, &args));

    let stop = Arc::new(AtomicBool::new(false));
    let for_ctrlc = stop.clone();
    ctrlc::set_handler(move || {
        for_ctrlc.store(true, Ordering::Relaxed);
    })
    .expect("Setting signal handler");

    while let Some(req) = session.next_request().await? {
        let fs = fs.clone();
        let _: JoinHandle<Result<()>> = task::spawn(async move {
            match req.operation()? {
                Operation::Lookup(op) => fs.lookup(&req, op).await?,
                Operation::Getattr(op) => fs.getattr(&req, op).await?,
                Operation::Setattr(op) => fs.setattr(&req, op).await?,
                Operation::Read(op) => fs.read(&req, op).await?,
                Operation::Readdir(op) => fs.readdir(&req, op).await?,
                Operation::Write(op, data) => fs.write(&req, op, data).await?,
                Operation::Mkdir(op) => fs.mkdir(&req, op).await?,
                Operation::Rmdir(op) => fs.rmdir(&req, op).await?,
                Operation::Mknod(op) => fs.mknod(&req, op).await?,
                Operation::Unlink(op) => fs.unlink(&req, op).await?,
                Operation::Rename(op) => fs.rename(&req, op).await?,
                Operation::Open(op) => fs.open(&req, op).await?,
                Operation::Flush(op) => fs.flush(&req, op).await?,
                Operation::Release(op) => fs.release(&req, op).await?,
                Operation::Fsync(op) => fs.fsync(&req, op).await?,
                Operation::Statfs(op) => fs.statfs(&req, op).await?,
                _ => {
                    error!("Unhandled op code in request {:?}", req.operation());
                    req.reply_error(libc::ENOSYS)?
                }
            }

            Ok(())
        });
        if stop.as_ref().load(Ordering::Relaxed) {
            debug!("Leaving fuse loop");
            break;
        }
    }
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
