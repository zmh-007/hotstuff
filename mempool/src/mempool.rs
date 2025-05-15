use crate::config::{Committee, Parameters};
use crate::helper::Helper;
use crate::payload_broadcaster::PayloadBroadcaster;
use crate::processor::{Processor, SerializedPayloadMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    Payload(Vec<u8>),
    PayloadRequest(Vec<Digest>, /* origin */ PublicKey),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConsensusMempoolMessage {
    /// The consensus notifies the mempool that it need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The consensus notifies the mempool of a round update.
    Cleanup(Round),
}

pub struct Mempool {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
    /// Send messages to consensus.
    tx_consensus: Sender<Digest>,
}

impl Mempool {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        rx_consensus: Receiver<ConsensusMempoolMessage>,
        tx_consensus: Sender<Digest>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        // Define a mempool instance.
        let mempool = Self {
            name,
            committee,
            parameters,
            store,
            tx_consensus,
        };

        // Spawn all mempool tasks.
        mempool.handle_consensus_messages(rx_consensus);
        mempool.handle_producer_payloads();
        mempool.handle_mempool_messages();

        info!(
            "Mempool successfully booted on {}",
            mempool
                .committee
                .mempool_address(&mempool.name)
                .expect("Our public key is not in the committee")
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from the consensus.
    fn handle_consensus_messages(&self, rx_consensus: Receiver<ConsensusMempoolMessage>) {
        // The `Synchronizer` is responsible to keep the mempool in sync with the others. It handles the commands
        // it receives from the consensus (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_consensus,
        );
    }

    /// Spawn all tasks responsible to handle producer payload.
    fn handle_producer_payloads(&self) {
        let (tx_payload_broadcaster, rx_payload_broadcaster) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .producer_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */ PayloadReceiverHandler { tx_payload_broadcaster },
        );

        // The payloads are sent to the `PayloadBroadcaster` that broadcasts
        // (in a reliable manner) the payload to all other mempools that share the same `id` as us. Finally,
        // it gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        PayloadBroadcaster::spawn(
            /* rx_payload */ rx_payload_broadcaster,
            /* tx_message */ tx_quorum_waiter,
            /* mempool_addresses */
            self.committee.broadcast_addresses(&self.name),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_payload */ tx_processor,
        );

        // The `Processor` stores the payload commitment. It then forwards the payload commitment to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_payload */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
        );

        info!("Mempool listening to producer payload on {}", address);
    }

    /// Spawn all tasks responsible to handle messages from other mempools.
    fn handle_mempool_messages(&self) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other mempools.
        let mut address = self
            .committee
            .mempool_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            MempoolReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        // The `Helper` is dedicated to reply to payload requests from other mempools.
        Helper::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the payload we receive from the other mempools. It then forwards the
        // payload digest to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
        );

        info!("Mempool listening to mempool messages on {}", address);
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct PayloadReceiverHandler {
    tx_payload_broadcaster: Sender<Vec<u8>>,
}

#[async_trait]
impl MessageHandler for PayloadReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the payload to the payload maker.
        self.tx_payload_broadcaster
            .send(message.to_vec())
            .await
            .expect("Failed to send payload");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming mempool messages.
#[derive(Clone)]
struct MempoolReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedPayloadMessage>,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(MempoolMessage::Payload(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Ok(MempoolMessage::PayloadRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
