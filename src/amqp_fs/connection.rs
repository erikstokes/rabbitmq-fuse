use std::fs::File;
use std::io::Read;

use lapin::PromiseChain;
use lapin::{tcp::AMQPUriTcpExt, Connection};

use crate::cli;

fn identity_from_file(p12_file: &str) -> native_tls::Identity {
    let mut f = File::open(p12_file).expect("Unable to open client cert");
    let mut key_cert = Vec::new();
    f.read_to_end(&mut key_cert)
        .expect("unable to read cleint cert");
    match native_tls::Identity::from_pkcs12(&key_cert, "") {
        Ok(ident) => ident,
        Err(..) => {
            let password = rpassword::prompt_password("Key password: ").unwrap();
            native_tls::Identity::from_pkcs12(&key_cert, &password).expect("Unable to decrypt key")
        }
    }
}

fn ca_chain_from_file(pem_file: &str) -> native_tls::Certificate {
    let mut f = File::open(pem_file).expect("Unable to open ca chain");
    let mut ca_chain = Vec::new();
    f.read_to_end(&mut ca_chain)
        .expect("Unable to read ca chain");
    native_tls::Certificate::from_pem(&ca_chain).expect("unable to parse certificate")
}

pub fn get_connection(
    args: &cli::Args,
    conn_props: lapin::ConnectionProperties,
) -> PromiseChain<Connection> {
    let uri = &args.rabbit_addr.parse::<lapin::uri::AMQPUri>().unwrap();

    let handshake = uri.connect().and_then(|stream| {
        let mut tls_builder = native_tls::TlsConnector::builder();
        tls_builder.identity(identity_from_file(
            &args.key,
            // "bunnies"
        ));
        tls_builder.add_root_certificate(ca_chain_from_file(&args.cert));
        tls_builder.danger_accept_invalid_hostnames(true);
        stream.into_native_tls(
            tls_builder.build().expect("tls config"),
            &uri.authority.host,
        )
    });

    Connection::connector(conn_props)(uri.clone(), handshake)
}
