use std::convert::TryFrom;
use std::convert::TryInto;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use zk::ToHash;
use zk::FrSerialization;
use crate::mempool::{SerializedTransaction};
use l0::Tx;

pub struct Processor;

impl Processor {
    pub fn spawn(
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_transaction: Receiver<SerializedTransaction>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            while let Some(tx_bytes) = rx_transaction.recv().await {
                let tx = Tx::try_from(&tx_bytes[..]).expect("Failed to deserialize transaction from bytes");
                // Hash the transaction.
                let mut b = Vec::new();
                tx.hash().serialize_be_compressed(&mut b).expect("Failed to serialize transaction hash to bytes");

                // Store the transaction.
                store.write_tx(b.clone(), tx_bytes).await;

                tx_digest.send(Digest(b.try_into().expect("Failed to convert transaction hash bytes to digest"))).await.expect("Failed to send digest");
            }
        });
    }
}
