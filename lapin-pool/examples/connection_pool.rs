use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use lapin_pool::ConnectionBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let pool = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
        .plain_auth("rabbit")
        .with_password("rabbitpw")
        .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .pool()? // will fail if we can read the opener arguments
        .max_size(10) // these are now deadpool options
        .build()?;

    let connection = pool.get().await?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");

    // If the connection close, we just get another one!
    connection.close(0, "Closing connection").await?;

    let connection = pool.get().await?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");

    Ok(())
}
