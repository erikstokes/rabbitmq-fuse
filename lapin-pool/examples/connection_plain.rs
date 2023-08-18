use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use lapin_pool::ConnectionBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
        .with_plain_auth("rabbit", Some("rabbitpw"))
        .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .opener()?;

    let connection = opener.get_connection().await?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}
