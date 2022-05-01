#![warn(clippy::all)]

use anyhow::{ensure, Result};
use std::{ sync::Arc};
use tokio::{
    sync::Mutex,
    task::{self, JoinHandle},
};

use polyfuse::{KernelConfig, Operation};


#[allow(unused_imports)] use tracing::{info, error, debug, Level};
use tracing_subscriber;

use clap::Parser;

mod amqp_fs;
mod session;
mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // let mut args = pico_args::Arguments::from_env();
    let args = cli::Args::parse();

    // let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    ensure!(args.mountpoint.is_dir(), "mountpoint must be a directory");

    info!("Mounting RabbitMQ server {host} at {mount}/",
          mount=&args.mountpoint.display(), host=args.rabbit_addr);

    let session = session::AsyncSession::mount(args.mountpoint, KernelConfig::default()).await?;

    let fs = Arc::new(Mutex::new(amqp_fs::Rabbit::new(&args.rabbit_addr).await));

    while let Some(req) = session.next_request().await? {
        let mut fs = fs.clone();
        let _: JoinHandle<Result<()>> = task::spawn(async move {
            match req.operation()? {
                Operation::Lookup(op) => fs.lock().await.lookup(&req, op).await?,
                Operation::Getattr(op) => fs.lock().await.getattr(&req, op).await?,
                Operation::Read(op) => fs.lock().await.read(&req, op).await?,
                Operation::Readdir(op) => fs.lock().await.readdir(&req, op).await?,
                Operation::Write(op, data) => fs.lock().await.write(&req, op, data).await?,
                Operation::Mkdir(op) => fs.lock().await.mkdir(&req, op).await?,
                _ => req.reply_error(libc::ENOSYS)?,
            }

            Ok(())
        });
    }

    Ok(())
}
