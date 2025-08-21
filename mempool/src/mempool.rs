use crate::config::{Committee, Parameters};
use crate::helper::Helper;
use crate::processor::Processor;
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use crate::tx_broadcaster::PayloadBroadcaster;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use l0::{Tx, Wp};
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use serde::{Serialize, Deserialize};

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

pub type SerializedTransaction = Vec<u8>;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    Transaction(SerializedTransaction),
    TransactionRequest(Vec<Digest>, /* origin */ PublicKey),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConsensusMempoolMessage {
    /// The consensus notifies the mempool that it need to sync the target missing tx.
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
    /// Verify tx channel
    tx_verify: Sender<(Wp<Tx>, oneshot::Sender<bool>)>,
}

impl Mempool {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        rx_consensus: Receiver<ConsensusMempoolMessage>,
        tx_consensus: Sender<Digest>,
        tx_verify: Sender<(Wp<Tx>, oneshot::Sender<bool>)>,
    ) -> Sender<SerializedTransaction> {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        // Define a mempool instance.
        let mempool = Self {
            name,
            committee,
            parameters,
            store,
            tx_consensus,
            tx_verify,
        };

        // Spawn all mempool tasks.
        mempool.handle_consensus_messages(rx_consensus);
        let tx_transaction_broadcaster = mempool.handle_clients_transactions();
        mempool.handle_mempool_messages();

        info!(
            "Mempool successfully booted on {}",
            mempool
                .committee
                .mempool_address(&mempool.name)
                .expect("Our public key is not in the committee")
                .ip()
        );
        tx_transaction_broadcaster
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

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self) -> Sender<SerializedTransaction> {
        let (tx_transaction_broadcaster, rx_transaction_broadcaster) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .transactions_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_transaction_broadcaster: tx_transaction_broadcaster.clone() },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other mempools that share the same `id` as us. Finally,
        // it gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        PayloadBroadcaster::spawn(
            /* rx_transaction */ rx_transaction_broadcaster,
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
            /* tx_transaction */ tx_processor,
        );

        // The `Processor` hashes and stores the transaction. It then forwards the batch's digest to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_transaction */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
            self.tx_verify.clone(),
        );

        info!("Mempool listening to client transactions on {}", address);
        tx_transaction_broadcaster
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

        // The `Helper` is dedicated to reply to batch requests from other mempools.
        Helper::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the transactions we receive from the other mempools. It then forwards the
        // batch's digest to the consensus.
        Processor::spawn(
            self.store.clone(),
            /* rx_transaction */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
            self.tx_verify.clone(),
        );

        info!("Mempool listening to mempool messages on {}", address);
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_transaction_broadcaster: Sender<SerializedTransaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to transaction_broadcaster.
        self.tx_transaction_broadcaster
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming mempool messages.
#[derive(Clone)]
struct MempoolReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedTransaction>,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(MempoolMessage::Transaction(transaction)) => self
                .tx_processor
                .send(transaction)
                .await
                .expect("Failed to send transaction"),
            Ok(MempoolMessage::TransactionRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send transaction request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
