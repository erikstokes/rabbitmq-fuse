//! Functions for managing TLS rabbit connections
use std::{fs::File, sync::Arc};
use std::io::Read;

use deadpool::{async_trait,
               managed};

use lapin::{Connection,
            uri::AMQPUri,
            tcp::AMQPUriTcpExt, ConnectionProperties,
};
use native_tls::TlsConnector;
use tracing::info;

use crate::cli;


type RecycleResult = managed::RecycleResult<lapin::Error>;
type RecycleError = managed::RecycleError<lapin::Error>;

pub struct ConnectionManager {
    uri: AMQPUri,
    properties: ConnectionProperties,
    connector: Option<Arc<TlsConnector>>,
}

impl ConnectionManager {
    fn new(uri: &str,
           connector: Option<Arc<TlsConnector>>,
           properties: ConnectionProperties) -> Self{
        Self{uri: uri.parse::<lapin::uri::AMQPUri>().unwrap(),
             properties,
             connector,
             }
    }

    pub fn from_command_line(args: &cli::Args,
                             properties: ConnectionProperties) -> Self {

        let mut tls_builder = native_tls::TlsConnector::builder();
        tls_builder.identity(identity_from_file(&args.tls_options.key,
                                                &args.tls_options.password));
        tls_builder.add_root_certificate(ca_chain_from_file(&args.tls_options.cert));
        tls_builder.danger_accept_invalid_hostnames(true);
        let connector = Arc::new(tls_builder.build().expect("tls connector"));

        Self::new(&args.rabbit_addr,
                  Some(connector),
                  properties,
        )


    }


    async fn get_connection(
        &self,
    ) -> lapin::Result<Connection> {
        if let Some(connector) = self.connector.clone() {
            let connect =  move | uri: &AMQPUri | {
                uri.clone().connect().and_then(|stream| {
                    stream.into_native_tls(
                        &connector,
                        // &builder.build().expect("tls config"),
                        &uri.authority.host,
                    )
                })
            };

            Connection::connector(self.uri.clone(),
                                  Box::new(connect),
                                  self.properties.clone()).await
        } else {
            unimplemented!("Non-tls connections not implemented");
}
        }

}

#[async_trait]
impl managed::Manager for ConnectionManager {
    type Type = lapin::Connection;
    type Error = lapin::Error;

    async fn create(&self) ->  lapin::Result<Self::Type> {
        info!("Opening new connection");
        self.get_connection().await
    }

    // copypasta from https://github.com/bikeshedder/deadpool/blob/d7167eaf47ccaadabfb831ce3718cdebe51185ba/lapin/src/lib.rs#L91
    async fn recycle(&self, conn: &mut lapin::Connection) -> RecycleResult {
        match conn.status().state() {
            lapin::ConnectionState::Connected => Ok(()),
            other_state => Err(RecycleError::Message(format!(
                "lapin connection is in state: {:?}",
                other_state
            ))),
        }
    }
}

pub(crate) type ConnectionPool = managed::Pool<ConnectionManager>;

/// Load a TLS identity from p12 formatted file path
fn identity_from_file(p12_file: &str, password: &Option<String>) -> native_tls::Identity {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert)
        .expect("unable to read cleint cert");
    match native_tls::Identity::from_pkcs12(&key_cert, password.as_ref().unwrap_or(&"".to_string()))
    {
        Ok(ident) => ident,
        Err(..) => {
            let password = rpassword::prompt_password("Key password: ").unwrap();
            native_tls::Identity::from_pkcs12(&key_cert, &password).expect("Unable to decrypt key")
        }
    }
}

/// Load a certificate authority from a PEM formatted file path
fn ca_chain_from_file(pem_file: &str) -> native_tls::Certificate {
    let mut f = File::open(pem_file).expect("Unable to open ca chain");
    let mut ca_chain = Vec::new();
    f.read_to_end(&mut ca_chain)
        .expect("Unable to read ca chain");
    native_tls::Certificate::from_pem(&ca_chain).expect("unable to parse certificate")
}
