use std::collections::BTreeMap;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::TcpListener,
};
use tracing::{debug, error, info};

#[nutype::nutype(derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord))]
struct Timestamp(i32);

#[nutype::nutype(derive(Copy, Clone, Debug))]
struct Price(i32);

enum Command {
    Insert(Timestamp, Price),
    Query(Timestamp, Timestamp),
}

struct Client {
    prices: BTreeMap<Timestamp, Price>,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Invalid command")]
    InvalidCommand,
    #[error("Could not read from socket")]
    SocketRead(tokio::io::Error),
    #[error("Could not write to socket")]
    SocketWrite(tokio::io::Error),
    #[error("Calculating mean overflowed")]
    MeanOverflow(std::num::TryFromIntError),
}

impl Client {
    async fn read_command(&mut self) -> Result<Command, Error> {
        let mut char_buf = [0u8; 1];
        self.reader
            .read_exact(&mut char_buf)
            .await
            .map_err(Error::SocketRead)?;
        let read_char = char_buf[0] as char;

        let mut int_buf = [0u8; 4];
        self.reader
            .read_exact(&mut int_buf)
            .await
            .map_err(Error::SocketRead)?;
        let first_i32 = i32::from_be_bytes(int_buf);

        self.reader
            .read_exact(&mut int_buf)
            .await
            .map_err(Error::SocketRead)?;
        let second_i32 = i32::from_be_bytes(int_buf);

        let command = match read_char {
            'I' => Command::Insert(Timestamp::new(first_i32), Price::new(second_i32)),
            'Q' => Command::Query(Timestamp::new(first_i32), Timestamp::new(second_i32)),
            _ => return Err(Error::InvalidCommand),
        };
        Ok(command)
    }

    async fn write_i32(&mut self, value: i32) -> tokio::io::Result<()> {
        let bytes = value.to_be_bytes();
        self.writer.write_all(&bytes).await?;
        Ok(())
    }

    async fn process(&mut self) -> Result<(), Error> {
        loop {
            match self.read_command().await? {
                Command::Insert(timestamp, price) => {
                    debug!("Inserting price: {:?} at timestamp: {:?}", price, timestamp);
                    self.prices.insert(timestamp, price);
                }
                Command::Query(start, end) => {
                    let prices = if start <= end {
                        debug!("Querying prices between: {:?} and {:?}", start, end);
                        self.prices
                            .range(start..=end)
                            .map(|(_, price)| i128::from(price.into_inner()))
                            .collect()
                    } else {
                        Vec::new()
                    };

                    let mean: i32 = if prices.is_empty() {
                        0
                    } else {
                        prices.iter().sum::<i128>() / prices.len() as i128
                    }
                    .try_into()
                    .map_err(Error::MeanOverflow)?;

                    self.write_i32(mean).await.map_err(Error::SocketWrite)?;
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let listener = TcpListener::bind("127.0.0.1:1337").await?;
    info!("Server listening on 127.0.0.1:1337");

    loop {
        let (socket, _) = listener.accept().await?;
        info!("Accepted new connection");

        tokio::spawn(async move {
            let (reader, writer) = socket.into_split();
            let mut client = Client {
                prices: BTreeMap::new(),
                reader: BufReader::new(reader),
                writer,
            };

            if let Err(e) = client.process().await {
                error!("Error processing client: {:?}", e);
            }
        });
    }
}
