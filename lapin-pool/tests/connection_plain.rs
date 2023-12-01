use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn main() -> Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let props =
        lapin::ConnectionProperties::default().with_connection_name("Test Connection".into());

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
        .with_properties(props)
        .plain_auth("guest")
        .with_password("guest")
        // .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .opener()?;

    let connection = opener.get_connection().await.into_diagnostic()?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await.into_diagnostic()?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}

#[cfg(feature = "deadpool")]
mod pool_tests {
    use super::*;
    #[tokio::test]
    async fn pool() -> eyre::Result<()> {
        let props = lapin::ConnectionProperties::default()
            .with_connection_name("Pool Test Connection".into());

        let opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
            .with_properties(props)
            .plain_auth("guest")
            .with_password("guest")
            // .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
            .pool()?;
        Ok(())
    }
}
