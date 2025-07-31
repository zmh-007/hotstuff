use log::{debug, error, info, warn};
use mempool::{SerializedTransaction};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{
    accept_async, 
    tungstenite::Message as WsMessage
};
use consensus::{Block, FullBlock, WebSocketEvent};
use futures::{SinkExt, StreamExt};


#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    SubscribeChainUpdate,
    UnsubscribeChainUpdate,
    ChainUpdate(Vec<u8>),
    SendTransactions(Vec<Vec<u8>>),
    RequestBlocks(Vec<Vec<u8>>),
    Blocks(Vec<Vec<u8>>),
}

#[derive(Debug)]
pub enum WebSocketError {
    SerializationError(Box<bincode::ErrorKind>),
    NetworkError(std::io::Error),
    // MessageError(String),
}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WebSocketError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            WebSocketError::NetworkError(e) => write!(f, "Network error: {}", e),
            // WebSocketError::MessageError(e) => write!(f, "Message error: {}", e),
        }
    }
}

impl std::error::Error for WebSocketError {}

impl From<Box<bincode::ErrorKind>> for WebSocketError {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        WebSocketError::SerializationError(error)
    }
}

impl From<std::io::Error> for WebSocketError {
    fn from(error: std::io::Error) -> Self {
        WebSocketError::NetworkError(error)
    }
}

pub struct ClientConnection {
    // pub id: String,
    pub sender: mpsc::UnboundedSender<Message>,
    pub subscribed: bool,
}

pub enum ServerMessage {
    NewClient {
        id: String,
        sender: mpsc::UnboundedSender<Message>,
    },
    ClientDisconnected {
        id: String,
    },
    Subscribe {
        client_id: String,
    },
    Unsubscribe {
        client_id: String,
    },
    SendBlocks {
        client_id: String,
        blocks: Vec<Vec<u8>>,
    },
}

pub struct WebSocketServer {
    store: Store,
    mempool_tx: mpsc::Sender<SerializedTransaction>,
    event_receiver: Option<mpsc::Receiver<WebSocketEvent>>,
    clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    message_sender: mpsc::UnboundedSender<ServerMessage>,
    message_receiver: Option<mpsc::UnboundedReceiver<ServerMessage>>,
}

impl WebSocketServer {
    pub fn new(store: Store, mempool_tx: mpsc::Sender<SerializedTransaction>, event_receiver: mpsc::Receiver<WebSocketEvent>) -> Self {
        let (message_sender, message_receiver) = mpsc::unbounded_channel();
        Self {
            store,
            mempool_tx,
            event_receiver: Some(event_receiver),
            clients: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            message_receiver: Some(message_receiver),
        }
    }

    pub async fn start(&mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        info!("WebSocket server listening on: {}", addr);
        
        let message_receiver = self.message_receiver.take().unwrap();
        let event_receiver = self.event_receiver.take().unwrap();
        let clients = self.clients.clone();
        let clients_for_messages = clients.clone();
        tokio::spawn(async move {
            Self::handle_server_messages(message_receiver, clients_for_messages).await;
        });
        let clients_for_events = clients.clone();
        tokio::spawn(async move {
            Self::handle_websocket_events(event_receiver, clients_for_events).await;
        });
        let mempool_tx = self.mempool_tx.clone();
        let message_sender = self.message_sender.clone();
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New connection from: {}", addr);
                    let store_clone = self.store.clone();
                    let mempool_tx_clone = mempool_tx.clone();
                    let message_sender_clone = message_sender.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream, 
                            store_clone, 
                            mempool_tx_clone, 
                            message_sender_clone
                        ).await {
                            error!("Error handling connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream,
        store: Store,
        mempool_tx: mpsc::Sender<SerializedTransaction>,
        message_sender: mpsc::UnboundedSender<ServerMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ws_stream = accept_async(stream).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let client_id = uuid::Uuid::new_v4().to_string();
        let (client_tx, mut client_rx) = mpsc::unbounded_channel::<Message>();
        let _ = message_sender.send(ServerMessage::NewClient {
            id: client_id.clone(),
            sender: client_tx.clone(),
        });
        info!("Client {} connected", client_id);

        let client_id_for_receive = client_id.clone();
        let mempool_tx_for_receive = mempool_tx.clone();
        let message_sender_for_receive = message_sender.clone();
        let mut store_for_receive = store.clone();
        
        let receive_task = tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(WsMessage::Text(text)) => {
                        match serde_json::from_str::<Message>(&text) {
                            Ok(message) => {
                                Self::handle_client_message(
                                    message,
                                    &mut store_for_receive,
                                    &mempool_tx_for_receive,
                                    &message_sender_for_receive,
                                    &client_id_for_receive,
                                ).await;
                            }
                            Err(e) => {
                                error!("Failed to parse message from client {}: {}", client_id_for_receive, e);
                            }
                        }
                    }
                    Ok(WsMessage::Binary(data)) => {
                        match bincode::deserialize::<Message>(&data) {
                            Ok(message) => {
                                Self::handle_client_message(
                                    message,
                                    &mut store_for_receive,
                                    &mempool_tx_for_receive,
                                    &message_sender_for_receive,
                                    &client_id_for_receive,
                                ).await;
                            }
                            Err(e) => {
                                error!("Failed to parse binary message from client {}: {}", client_id_for_receive, e);
                            }
                        }
                    }
                    Ok(WsMessage::Close(_)) => {
                        info!("Client {} disconnected", client_id_for_receive);
                        break;
                    }
                    Err(e) => {
                        error!("WebSocket error for client {}: {}", client_id_for_receive, e);
                        break;
                    }
                    _ => {}
                }
            }
        });
        let client_id_for_send = client_id.clone();
        let send_task = tokio::spawn(async move {
            while let Some(message) = client_rx.recv().await {
                if let Err(e) = ws_sender.send(serde_json::to_string(&message).unwrap().into()).await {
                    error!("Failed to send message to client {}: {}", client_id_for_send, e);
                    break;
                }
            }
        });

        tokio::select! {
            _ = receive_task => {},
            _ = send_task => {},
        }

        let _ = message_sender.send(ServerMessage::ClientDisconnected {
            id: client_id.clone(),
        });
        
        info!("Connection handler for client {} finished", client_id);
        Ok(())
    }

    async fn handle_server_messages(
        mut message_receiver: mpsc::UnboundedReceiver<ServerMessage>,
        clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    ) {
        while let Some(message) = message_receiver.recv().await {
            match message {
                ServerMessage::NewClient { id, sender } => {
                    let mut clients_guard = clients.write().await;
                    clients_guard.insert(id.clone(), ClientConnection {
                        // id: id.clone(),
                        sender,
                        subscribed: false,
                    });
                    debug!("Registered new client: {}", id);
                }
                
                ServerMessage::ClientDisconnected { id } => {
                    let mut clients_guard = clients.write().await;
                    clients_guard.remove(&id);
                    debug!("Removed client: {}", id);
                }
                
                ServerMessage::Subscribe { client_id } => {
                    let mut clients_guard = clients.write().await;
                    if let Some(client) = clients_guard.get_mut(&client_id) {
                        client.subscribed = true;
                        debug!("Client {} subscribed to chain updates", client_id);
                    }
                }
                
                ServerMessage::Unsubscribe { client_id } => {
                    let mut clients_guard = clients.write().await;
                    if let Some(client) = clients_guard.get_mut(&client_id) {
                        client.subscribed = false;
                        debug!("Client {} unsubscribed from chain updates", client_id);
                    }
                }
                
                ServerMessage::SendBlocks { client_id, blocks } => {
                    let clients_guard = clients.read().await;
                    if let Some(client) = clients_guard.get(&client_id) {
                        let response_msg = Message::Blocks(blocks);
                        if let Err(e) = client.sender.send(response_msg) {
                            error!("Failed to send Blocks to client {}: {}", client_id, e);
                        }
                    } else {
                        warn!("Client {} not found when sending Blocks", client_id);
                    }
                }
            }
        }
    }

    async fn handle_client_message(
        message: Message,
        store: &mut Store,
        mempool_tx: &mpsc::Sender<SerializedTransaction>,
        message_sender: &mpsc::UnboundedSender<ServerMessage>,
        client_id: &str,
    ) {
        match message {
            Message::SubscribeChainUpdate => {
                debug!("Client {} subscribed to chain updates", client_id);
                let _ = message_sender.send(ServerMessage::Subscribe {
                    client_id: client_id.to_string(),
                });
            }
            
            Message::UnsubscribeChainUpdate => {
                debug!("Client {} unsubscribed from chain updates", client_id);
                let _ = message_sender.send(ServerMessage::Unsubscribe {
                    client_id: client_id.to_string(),
                });
            }
            
            Message::SendTransactions(transactions) => {
                debug!("Received {} transactions from client {}", transactions.len(), client_id);
                for tx_bytes in transactions {
                    let serialized_transaction: SerializedTransaction = tx_bytes;
                    if let Err(e) = mempool_tx.send(serialized_transaction).await {
                        error!("Failed to send transaction to mempool: {}", e);
                    }
                }
            }
            
            Message::RequestBlocks(hashes) => {
                debug!("Requesting {} blocks by prev_hash", hashes.len());
                let mut blocks = Vec::new();
                for hash in hashes {
                    if let Some(block_data) = Self::get_block_by_hash(store, &hash).await {
                        if let Ok(block) = bincode::deserialize::<Block>(&block_data) {
                            let mut txs = Vec::new();
                            for tx_hash in block.payload {
                                if let Some(tx_data) = Self::get_tx_by_hash(store, &hash).await {
                                    txs.push(tx_data);
                                }
                                else {
                                    error!("Transaction not found for hash: {:?}", tx_hash);
                                }
                            }
                            let full_block = FullBlock {
                                qc: block.qc.clone(),
                                tc: block.tc.clone(),
                                author: block.author.clone(),
                                round: block.round,
                                payload: txs,
                                txg: block.txg.clone(),
                                next: block.next.clone(),
                                signature: block.signature.clone(),
                            };
                            blocks.push(bincode::serialize(&full_block).expect("Failed to serialize full block"));
                        } else {
                            error!("Failed to deserialize block data for hash: {:?}", hash);
                        }
                    } else {
                        debug!("Block not found for hash: {:?}", hash);
                    }
                }
                if blocks.is_empty() {
                    return;
                }
                
                let _ = message_sender.send(ServerMessage::SendBlocks {
                    client_id: client_id.to_string(),
                    blocks,
                });
            }
            
            Message::Blocks(_) => {
            }
            
            Message::ChainUpdate(_) => {
            }
            
        }
    }

    async fn get_block_by_hash(
        store: &mut Store,
        hash: &Vec<u8>,
    ) -> Option<Vec<u8>> {
        match store.read(hash.to_vec()).await {
            Ok(Some(data)) => {
                debug!("Successfully retrieved block data for hash: {:?}", hash);
                Some(data)
            },
            Ok(None) => {
                debug!("No block data found for hash: {:?}", hash);
                None
            },
            Err(e) => {
                error!("Error getting block data for hash {:?}: {}", hash, e);
                None
            }
        }
    }

    async fn get_tx_by_hash(
        store: &mut Store,
        hash: &Vec<u8>,
    ) -> Option<Vec<u8>> {
        match store.read(hash.to_vec()).await {
            Ok(Some(data)) => {
                debug!("Successfully retrieved tx data for hash: {:?}", hash);
                Some(data)
            },
            Ok(None) => {
                debug!("No tx data found for hash: {:?}", hash);
                None
            },
            Err(e) => {
                error!("Error getting tx data for hash {:?}: {}", hash, e);
                None
            }
        }
    }

    async fn handle_websocket_events(
        mut event_receiver: mpsc::Receiver<WebSocketEvent>,
        clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    ) {
        while let Some(event) = event_receiver.recv().await {
            match event {
                WebSocketEvent::BroadcastChainUpdate { hash } => {
                    let clients = clients.read().await;
                    let message = Message::ChainUpdate(hash);
                    for (client_id, client) in clients.iter() {
                        if client.subscribed {
                            if let Err(e) = client.sender.send(message.clone()) {
                                error!("Failed to send chain update to client {}: {}", client_id, e);
                            }
                        }
                    }
                }
            }
        }
        debug!("WebSocket event handler stopped");
    }
}