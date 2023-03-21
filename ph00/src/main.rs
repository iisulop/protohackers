use std::net::SocketAddr;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use thiserror::Error;
use tokio::spawn;
use tracing::{info, Level, debug, trace_span, trace, error};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::Subscriber;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum MsgrError {
    #[error("Cannot read from socket")]
    SocketReadError(#[from] std::io::Error),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let subscriber = Subscriber::builder()
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Could not initialize tracing");
    server().await;
}

async fn server() {
    let addr = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
    debug!("Starting listener on address {addr:#?}");
    let listener = TcpListener::bind(addr).await.unwrap();
    info!("Started listener on address {:#?}", listener.local_addr());
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        spawn(async move { connection_listener(socket).await });
    }
}

async fn write_message(socket: &mut TcpStream, contents: &[u8]) -> Result<(), MsgrError> {
    debug!("Sending message \"{:?}\"", &contents);
    Ok(socket.write_all(contents).await?)
}

async fn connection_listener(mut socket: TcpStream) {
    let peer = socket.peer_addr();
    let connection_span = trace_span!("listening on connection", ?peer);
    let _enter = connection_span.enter();
    let mut buf = vec![0; 1024];
    loop {
        #[allow(unused_must_use)]
        match read_incoming_bytes(&mut socket, &mut buf).await {
            Ok(0) => {
                info!("Socket {:?} closed", socket.peer_addr());
                break;
            }
            Ok(n) => {
                trace!("Read {n} bytes from socket {:?}", socket.peer_addr());
                let message = &buf[0..n];
                println!("Got message:\n{:?}", message);
                if let Err(err) = write_message(&mut socket, message).await {
                    error!("Could not send message: {:#?}", err);
                }
            }
            Err(err) => error!("Could not deserialize message: {:#?}", err),
        };
    }
}

async fn read_incoming_bytes(socket: &mut TcpStream, buf: &mut [u8]) -> Result<usize, MsgrError> {
    trace!("Processing incoming from {:?}", socket.peer_addr());
    let n = socket.read(buf).await?;
    Ok(n)
}
