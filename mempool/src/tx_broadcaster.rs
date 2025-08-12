use crate::mempool::{MempoolMessage, SerializedTransaction};
use crate::quorum_waiter::QuorumWaiterMessage;
use bytes::Bytes;
use crypto::PublicKey;
use network::ReliableSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};

/// broadcast payloads.
pub struct PayloadBroadcaster {
    /// Channel to receive payload from the network.
    rx_transaction: Receiver<SerializedTransaction>,
    /// Output channel to deliver payload to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl PayloadBroadcaster {
    pub fn spawn(
        rx_transaction: Receiver<SerializedTransaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                rx_transaction,
                tx_message,
                mempool_addresses,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming payload.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(transaction) = self.rx_transaction.recv() => {
                    self.broadcast(transaction).await;
                },
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// broadcast the transaction.
    async fn broadcast(&mut self, transaction: SerializedTransaction) {
        let message = MempoolMessage::Transaction(transaction.clone());
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own transaction");

        // Broadcast the transaction through the network.
        let (names, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the transaction through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                transaction: transaction,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver payload");
    }
}