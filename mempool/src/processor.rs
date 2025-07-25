use std::convert::TryFrom;
use crypto::Digest;
use store::Store;
use bincode::deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::mempool::{SerializedTransaction, TransactionFields};
use l0::Transaction;

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
                let tf: TransactionFields = deserialize(&tx_bytes).unwrap();
                let tx = Transaction::try_from(tf.0).unwrap();
                // Hash the transaction.
                let digest = Digest(tx.hash().into());

                // Store the transaction.
                store.write(digest.to_vec(), tx_bytes).await;

                tx_digest.send(digest).await.expect("Failed to send digest");
            }
        });
    }
}
