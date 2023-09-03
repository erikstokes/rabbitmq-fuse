use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5671/%2f")
        .external_auth()
        .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .with_p12("../test_all//tls-gen/basic/client_rabbit/keycert.p12")
        .password_prompt()
        .opener()?;

    let connection = opener.get_connection().await.into_diagnostic()?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await.into_diagnostic()?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}
