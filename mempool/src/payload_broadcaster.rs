use crate::mempool::MempoolMessage;
use crate::quorum_waiter::QuorumWaiterMessage;
use bytes::Bytes;
use crypto::PublicKey;
use network::ReliableSender;
use std::net::SocketAddr;
#[cfg(feature = "benchmark")]
use crypto::Digest;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use tokio::sync::mpsc::{Receiver, Sender};

/// broadcast payloads.
pub struct PayloadBroadcaster {
    /// Channel to receive payload from the network.
    rx_payload: Receiver<Vec<u8>>,
    /// Output channel to deliver payload to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl PayloadBroadcaster {
    pub fn spawn(
        rx_payload: Receiver<Vec<u8>>,
        tx_message: Sender<QuorumWaiterMessage>,
        mempool_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                rx_payload,
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
                // Assemble client transactions into batches of preset size.
                Some(payload) = self.rx_payload.recv() => {
                    self.broadcast(payload).await;
                },
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// broadcast the payload.
    async fn broadcast(&mut self, payload: Vec<u8>) {
        // Serialize the payload.
        let message = MempoolMessage::Payload(payload.clone());
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own payload");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );
            let id: [u8; 8] = payload[0..8].try_into().unwrap();
            // NOTE: This log entry is used to compute performance.
            info!(
                "Hash {:?} contains payload {}",
                digest,
                u64::from_be_bytes(id)
            );
        }

        // Broadcast the payload through the network.
        let (names, addresses): (Vec<_>, _) = self.mempool_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the payload through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                payload: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver payload");
    }
}
