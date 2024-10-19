use std::collections::HashMap;

use nutype::nutype;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc,
};
use tracing::{debug, error, info, trace, warn};

const NAME_PROMPT: &[u8] = b"Welcome to budgetchat! What shall I call you? \n";

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Invalid name")]
    InvalidName(#[from] ClientNameError),
    #[error("Error joining threads")]
    Join(#[from] tokio::task::JoinError),
    #[error("Error writing to TCP stream")]
    TcpWrite(#[from] std::io::Error),
    #[error("Error sending message to server")]
    Send(#[from] mpsc::error::SendError<ClientServerMessage>),
}

#[nutype(
    sanitize(trim),
    validate(regex = r"^[a-zA-Z0-9]+$"),
    derive(Clone, Debug, Display, Eq, Hash, PartialEq)
)]
struct ClientName(String);

enum ClientServerMessage {
    Message {
        client_name: ClientName,
        content: String,
    },
    Register {
        client_name: ClientName,
        client_channel: mpsc::Sender<ServerClientMessage>,
    },
    Unregister {
        client_name: ClientName,
    },
}

struct RegisterChannel {
    receiver: mpsc::Receiver<ClientServerMessage>,
    sender: mpsc::Sender<ClientServerMessage>,
}

impl RegisterChannel {
    fn new(
        receiver: mpsc::Receiver<ClientServerMessage>,
        sender: mpsc::Sender<ClientServerMessage>,
    ) -> Self {
        Self { receiver, sender }
    }
}

#[derive(Clone, Debug)]
enum MessageSource {
    Client(ClientName),
    Server,
}

#[derive(Clone, Debug)]
enum ServerClientMessage {
    Message {
        source: MessageSource,
        content: String,
    },
}

struct Server {
    client_channels: HashMap<ClientName, mpsc::Sender<ServerClientMessage>>,
    receive_channel: RegisterChannel,
}

impl Server {
    fn new() -> Self {
        let (register_channel_sender, register_channel_receiver) = mpsc::channel(16);
        Self {
            client_channels: HashMap::new(),
            receive_channel: RegisterChannel::new(
                register_channel_receiver,
                register_channel_sender,
            ),
        }
    }

    fn add_client(
        &mut self,
        client_name: ClientName,
        client_channel: mpsc::Sender<ServerClientMessage>,
    ) {
        debug!("Adding client: {}", client_name);
        self.client_channels.insert(client_name, client_channel);
    }

    async fn process(mut self) -> Result<(), Error> {
        loop {
            match self.receive_channel.receiver.recv().await {
                Some(ClientServerMessage::Message {
                    client_name,
                    content,
                }) => {
                    debug!("Received message from {}: {}", client_name, content);
                    self.send_message_to_all_clients(ServerClientMessage::Message {
                        source: MessageSource::Client(client_name.clone()),
                        content: content.clone(),
                    })
                    .await;
                }
                Some(ClientServerMessage::Register {
                    client_name,
                    client_channel,
                }) => {
                    debug!("Registering client: {}", client_name);
                    let clients_in_room = self.client_channels.keys().fold(
                        "* the room contains:".to_string(),
                        |message, client_name| format!("{message}, {client_name}"),
                    );
                    self.send_message_to_all_clients(ServerClientMessage::Message {
                        source: MessageSource::Server,
                        content: format!("* {client_name} has entered the chat"),
                    })
                    .await;
                    trace!("Adding client to list");
                    self.add_client(client_name.clone(), client_channel);
                    self.send_message_to_client(
                        client_name.clone(),
                        ServerClientMessage::Message {
                            source: MessageSource::Server,
                            content: clients_in_room,
                        },
                    )
                    .await;
                }
                Some(ClientServerMessage::Unregister { client_name }) => {
                    debug!("Unregistering client: {}", client_name);
                    self.client_channels.remove_entry(&client_name);
                    self.send_message_to_all_clients(ServerClientMessage::Message {
                        source: MessageSource::Server,
                        content: format!("* {client_name} has left the chat"),
                    })
                    .await;
                }
                None => {
                    error!("Receive channel closed");
                    return Ok(());
                }
            }
        }
    }

    async fn send_message_to_all_clients(&self, message: ServerClientMessage) {
        trace!("Sending message to all clients: {:?}", message);
        for client_channel in self.client_channels.values() {
            let _ = client_channel.send(message.clone()).await;
        }
    }

    async fn send_message_to_client(&self, client_name: ClientName, message: ServerClientMessage) {
        trace!("Sending message to client {}: {:?}", client_name, message);
        if let Some(client_channel) = self.client_channels.get(&client_name) {
            trace!("Found channel");
            let _result = client_channel.send(message).await.inspect_err(|err| {
                warn!("Error sending message to client {}: {:?}", client_name, err);
            });
        } else {
            warn!("Could not send message to client {}", client_name);
        }
    }

    pub fn get_register_channel(&self) -> mpsc::Sender<ClientServerMessage> {
        self.receive_channel.sender.clone()
    }
}

struct ServerConnections {
    from_server_channel: mpsc::Receiver<ServerClientMessage>,
    to_server_channel: mpsc::Sender<ClientServerMessage>,
}

impl ServerConnections {
    fn new(
        from_server_channel: mpsc::Receiver<ServerClientMessage>,
        to_server_channel: mpsc::Sender<ClientServerMessage>,
    ) -> Self {
        Self {
            from_server_channel,
            to_server_channel,
        }
    }
}

struct Client {
    name: ClientName,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    server: ServerConnections,
}

async fn read_line(reader: &mut BufReader<OwnedReadHalf>) -> Result<String, Error> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await?;
    Ok(String::from_utf8_lossy(&line).into())
}

impl Client {
    async fn connect(
        socket: TcpStream,
        to_server_channel: mpsc::Sender<ClientServerMessage>,
    ) -> Result<Self, Error> {
        debug!("Connecting new client");
        let (reader, mut writer) = socket.into_split();
        let mut reader = BufReader::new(reader);
        writer.write_all(NAME_PROMPT).await?;
        trace!("Sent name prompt to client");
        let name = ClientName::try_new(read_line(&mut reader).await?)?;
        trace!("Received client name: {}", name);
        let (sender, receiver) = mpsc::channel(16);
        to_server_channel
            .send(ClientServerMessage::Register {
                client_name: name.clone(),
                client_channel: sender,
            })
            .await?;
        trace!("Registered client: {}", name);
        let server_connections = ServerConnections::new(receiver, to_server_channel);
        Ok(Client::new(name, reader, writer, server_connections))
    }

    fn new(
        name: ClientName,
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        server: ServerConnections,
    ) -> Self {
        trace!("Creating new client instance: {}", name);
        Self {
            name,
            reader,
            writer,
            server,
        }
    }

    async fn process(self) -> Result<(), Error> {
        let mut reader = self.reader;
        let server = self.server;
        let name = self.name.clone();

        let client: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            loop {
                let mut buf = String::new();
                match reader.read_line(&mut buf).await {
                    Ok(0) => {
                        // EOF
                        debug!("Client {} disconnected", name);
                        server
                            .to_server_channel
                            .send(ClientServerMessage::Unregister {
                                client_name: name.clone(),
                            })
                            .await?;
                        return Ok(());
                    }
                    Ok(_) => {
                        let content = buf.strip_suffix('\n').unwrap_or("").to_string();
                        debug!("Received message from client {}: {}", name, content);
                        server
                            .to_server_channel
                            .send(ClientServerMessage::Message {
                                client_name: name.clone(),
                                content,
                            })
                            .await?;
                    }
                    Err(_) => {
                        error!("Error reading from client {}", name);
                        server
                            .to_server_channel
                            .send(ClientServerMessage::Unregister {
                                client_name: name.clone(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
            }
        });

        let name = self.name.clone();
        let mut server_receiver = server.from_server_channel;
        let mut writer = self.writer;

        let server: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            loop {
                trace!("Waiting for message from server");
                match server_receiver.recv().await {
                    Some(ServerClientMessage::Message {
                        source: MessageSource::Client(source_client_name),
                        content,
                    }) => {
                        let message = format!("[{source_client_name}] {content}\n");
                        if source_client_name == name {
                            continue;
                        }
                        debug!(
                            "Sending message from {} to client {}: {}",
                            source_client_name, name, message
                        );
                        writer.write_all(message.as_bytes()).await?;
                    }
                    Some(ServerClientMessage::Message {
                        source: MessageSource::Server,
                        content,
                    }) => {
                        let message = format!("{content}\n");
                        debug!("Sending server message to client {}: {}", name, message);
                        writer.write_all(message.as_bytes()).await?;
                    }
                    None => {
                        error!("Server channel closed for client {}", name);
                        return Ok(());
                    }
                }
            }
        });

        let (server_result, client_result) = tokio::try_join!(server, client)?;
        server_result?;
        client_result?;
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    let listener = TcpListener::bind("127.0.0.1:1337").await?;
    info!("Server listening on 127.0.0.1:1337");
    let server = Server::new();
    let server_register_channel = server.get_register_channel();

    tokio::spawn(async move {
        let _result = server.process().await;
    });

    loop {
        let (socket, address) = listener.accept().await?;
        info!("Accepted new connection from {address:?}");
        let server_register_channel = server_register_channel.clone();
        let _handle: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            debug!("Starting new client task");
            match Client::connect(socket, server_register_channel).await {
                Ok(client) => {
                    if let Err(e) = client.process().await {
                        error!("Error processing client: {:?}", e);
                    }
                }
                Err(err) => error!("{}", err),
            };
            Ok(())
        });
    }
}
