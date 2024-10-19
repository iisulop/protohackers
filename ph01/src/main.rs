use is_prime::is_prime;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt as _;
use tokio::io::BufReader;
use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tracing::warn;

use std::net::AddrParseError;
use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tracing::{debug, error, info, trace, trace_span, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::Subscriber;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parsing client address")]
    ClientAddress(#[from] AddrParseError),
    #[error("Cannot read from socket")]
    SocketRead(#[from] std::io::Error),
    #[error("Serializing `IsPrimeResponse`")]
    Serialize(#[from] serde_json::Error)
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum Method {
    IsPrime,
}

#[derive(Deserialize)]
struct IsPrimeRequest {
    #[allow(dead_code)]
    method: Method,
    number: serde_json::value::Number,
}
impl IsPrimeRequest {
    fn get_reponse(&self) -> Result<String, Error> {
        println!("Got number: {:?}", self.number);
        let response = if self.number.as_str().contains('.') {
            // Decimal number
            println!("Number is decimal");
            IsPrimeResponse::correct(false)?
        } else if self.number.as_str().starts_with('-') {
            println!("Number is negative integer");
            IsPrimeResponse::correct(false)?
        } else {
            println!("Number is positive integer");
            IsPrimeResponse::correct(is_prime(self.number.as_str()))?
        };
        dbg!(&response);
        Ok(format!("{response}\n"))
    }
}

#[derive(Serialize)]
struct IsPrimeResponse {
    method: Method,
    prime: bool,
}

impl IsPrimeResponse {
    fn incorrect() -> String {
        "{}\n".to_string()
    }

    fn correct(prime: bool) -> Result<String, Error> {
        Ok(serde_json::to_string(&Self {
            method: Method::IsPrime,
            prime,
        })?)
    }
}

#[tokio::main]
async fn main() {
    let subscriber = Subscriber::builder()
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(Level::TRACE)
        .finish();
    match tracing::subscriber::set_global_default(subscriber) {
        Ok(()) => (),
        Err(err) => warn!("Failed to initialize tracing subscriber: {err:#?}"),
    }
    let _ = server().await;
}

async fn server() -> Result<(), Error> {
    let addr = "0.0.0.0:1337".parse::<SocketAddr>()?;
    debug!("Starting listener on address {addr:#?}");
    let listener = TcpListener::bind(addr).await?;
    info!("Started listener on address {:#?}", listener.local_addr());
    let mut num_connections = 0;
    loop {
        debug!("Waiting for connections");
        let (socket, _) = listener.accept().await?;
        num_connections += 1;
        debug!("Got connection # {num_connections}");
        tokio::spawn(async move { connection_listener(socket).await });
    }
}

async fn write_message(socket: &mut WriteHalf<'_>, contents: &[u8]) -> Result<(), Error> {
    debug!("Sending message `{:?}`", String::from_utf8_lossy(contents));
    debug!("Sending message `{:?}`", &contents);
    Ok(socket.write_all(contents).await?)
}

async fn connection_listener(mut socket: TcpStream) -> Result<(), Error> {
    let peer = socket.peer_addr();
    let connection_span = trace_span!("listening on connection", ?peer);
    let _enter = connection_span.enter();
    let peer_address = socket.peer_addr()?;
    let (read_socket, mut write_socket) = socket.split();
    let mut socket_reader = BufReader::new(read_socket);
    loop {
        let mut buf = Vec::new();
        #[allow(unused_must_use)]
        match read_incoming_bytes(peer_address, &mut socket_reader, &mut buf).await {
            Ok(0) => {
                info!("Socket {:?} closed", peer_address);
                break;
            }
            Ok(n) => {
                trace!("Read {n} bytes from socket {:?}", peer_address);
                let message = String::from_utf8_lossy(&buf[0..n]);
                println!("Got message:\n{message:?}");
                match serde_json::from_str::<IsPrimeRequest>(&message) {
                    Ok(request) => {
                        if let Ok(response) = request.get_reponse() {
                            if let Err(err) = write_message(&mut write_socket, response.as_bytes()).await {
                                error!("Could not send message: {:#?}", err);
                            }
                        }
                    }
                    Err(err) => {
                        info!("Error parsing request: {err:#?}");
                        write_message(&mut write_socket, IsPrimeResponse::incorrect().as_bytes()).await;
                    }
                }
            }
            Err(err) => error!("Could not deserialize message: {:#?}", err),
        };
    }
    Ok(())
}

async fn read_incoming_bytes(peer_address: SocketAddr, socket: &mut BufReader<ReadHalf<'_>>, buf: &mut Vec<u8>) -> Result<usize, Error> {
    trace!("Processing incoming from {:?}", peer_address);
    // let n = socket.read(buf).await?;
    let n = socket.read_until(b'\n', buf).await?;
    trace!("Read {n} bytes");
    Ok(n)
}
