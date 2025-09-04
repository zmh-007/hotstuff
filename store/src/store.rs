use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::oneshot;
use rocksdb::{DB, Options, ColumnFamilyDescriptor, Error};
use anyhow::Result;

pub type StoreError = Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

const BLOCKS_INDEX_CF: &str = "blocks_index";
const BLOCKS_CF: &str = "blocks";
const TRANSACTIONS_CF: &str = "transactions";
const CONSENSUS_CF: &str = "consensus";
const UTXO_CACHE_CF: &str = "utxo_cache";

const ROUND_PREFIX: &[u8] = b"round";
const LAST_VOTED_ROUND_PREFIX: &[u8] = b"last_voted_round";
const LAST_COMMITTED_ROUND_PREFIX: &[u8] = b"last_committed_round";
const QC_PREFIX: &[u8] = b"qc";
const UTXO_CACHE_PREFIX: &[u8] = b"utxo_cache";

pub enum StoreCommand {
    WriteBlockIndex(Key, Value),
    WriteBlock(Key, Value),
    WriteTransaction(Key, Value),
    WriteRound(u64),
    WriteLastVotedRound(u64),
    WriteLastCommittedRound(u64),
    WriteQC(Value),
    WriteUTXOCache(Value),

    ReadBlockIndex(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    ReadBlock(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    ReadTransaction(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    ReadRound(oneshot::Sender<StoreResult<Option<Value>>>),
    ReadLastVotedRound(oneshot::Sender<StoreResult<Option<Value>>>),
    ReadLastCommittedRound(oneshot::Sender<StoreResult<Option<Value>>>),
    ReadQC(oneshot::Sender<StoreResult<Option<Value>>>),
    ReadUTXOCache(oneshot::Sender<StoreResult<Option<Value>>>),
    
    NotifyReadBlock(Key, oneshot::Sender<StoreResult<Value>>),
    NotifyReadTransaction(Key, oneshot::Sender<StoreResult<Value>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub fn new(path: &str) -> StoreResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new(BLOCKS_INDEX_CF, Options::default()),
            ColumnFamilyDescriptor::new(BLOCKS_CF, Options::default()),
            ColumnFamilyDescriptor::new(TRANSACTIONS_CF, Options::default()),
            ColumnFamilyDescriptor::new(CONSENSUS_CF, Options::default()),
            ColumnFamilyDescriptor::new(UTXO_CACHE_CF, Options::default()),
        ];
        let db = Arc::new(DB::open_cf_descriptors(&opts, path, cfs)?);
        let (tx, rx) = channel(100);
        tokio::spawn(Self::process_commands(rx, db));
        Ok(Self { channel: tx })
    }

    async fn process_commands(
        mut rx: Receiver<StoreCommand>,
        db: Arc<DB>,
    ) {
        let mut block_obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let mut transaction_obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();

        while let Some(command) = rx.recv().await {
            let blocks_index_cf = db.cf_handle(BLOCKS_INDEX_CF).unwrap();
            let blocks_cf = db.cf_handle(BLOCKS_CF).unwrap();
            let transactions_cf = db.cf_handle(TRANSACTIONS_CF).unwrap();
            let consensus_cf = db.cf_handle(CONSENSUS_CF).unwrap();
            let utxo_cache_cf = db.cf_handle(UTXO_CACHE_CF).unwrap();

            match command {
                StoreCommand::WriteBlockIndex(key, value) => {
                    let _ = db.put_cf(&blocks_index_cf, &key, &value);
                }
                StoreCommand::WriteBlock(key, value) => {
                    let _ = db.put_cf(&blocks_cf, &key, &value);
                    if let Some(mut senders) = block_obligations.remove(&key) {
                        while let Some(s) = senders.pop_front() {
                            let _ = s.send(Ok(value.clone()));
                        }
                    }
                }
                StoreCommand::WriteTransaction(key, value) => {
                    let _ = db.put_cf(&transactions_cf, &key, &value);
                    if let Some(mut senders) = transaction_obligations.remove(&key) {
                        while let Some(s) = senders.pop_front() {
                            let _ = s.send(Ok(value.clone()));
                        }
                    }
                }
                StoreCommand::WriteRound(round) => {
                    let value = round.to_be_bytes().to_vec();
                    let _ = db.put_cf(&consensus_cf, ROUND_PREFIX, &value);
                }
                StoreCommand::WriteLastVotedRound(round) => {
                    let value = round.to_be_bytes().to_vec();
                    let _ = db.put_cf(&consensus_cf, LAST_VOTED_ROUND_PREFIX, &value);
                }
                StoreCommand::WriteLastCommittedRound(round) => {
                    let value = round.to_be_bytes().to_vec();
                    let _ = db.put_cf(&consensus_cf, LAST_COMMITTED_ROUND_PREFIX, &value);
                }
                StoreCommand::WriteQC(value) => {
                    let _ = db.put_cf(&consensus_cf, QC_PREFIX, &value);
                }
                StoreCommand::WriteUTXOCache(value) => {
                    let _ = db.put_cf(&utxo_cache_cf, UTXO_CACHE_PREFIX, &value);
                }
                StoreCommand::ReadBlockIndex(key, sender) => {
                    let response = db.get_cf(&blocks_index_cf, &key);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadBlock(key, sender) => {
                    let response = db.get_cf(&blocks_cf, &key);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadTransaction(key, sender) => {
                    let response = db.get_cf(&transactions_cf, &key);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadRound(sender) => {
                    let response = db.get_cf(&consensus_cf, ROUND_PREFIX);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadLastVotedRound(sender) => {
                    let response = db.get_cf(&consensus_cf, LAST_VOTED_ROUND_PREFIX);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadLastCommittedRound(sender) => {
                    let response = db.get_cf(&consensus_cf, LAST_COMMITTED_ROUND_PREFIX);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadQC(sender) => {
                    let response = db.get_cf(&consensus_cf, QC_PREFIX);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadUTXOCache(sender) => {
                    let response = db.get_cf(&utxo_cache_cf, UTXO_CACHE_PREFIX);
                    let _ = sender.send(response);
                }
                StoreCommand::NotifyReadBlock(key, sender) => {
                    let response = db.get_cf(&blocks_cf, &key);
                    match response {
                        Ok(None) => block_obligations
                            .entry(key)
                            .or_insert_with(VecDeque::new)
                            .push_back(sender),
                        _ => {
                            let _ = sender.send(response.map(|x| x.unwrap()));
                        }
                    }
                }
                StoreCommand::NotifyReadTransaction(key, sender) => {
                    let response = db.get_cf(&transactions_cf, &key);
                    match response {
                        Ok(None) => transaction_obligations
                            .entry(key)
                            .or_insert_with(VecDeque::new)
                            .push_back(sender),
                        _ => {
                            let _ = sender.send(response.map(|x| x.unwrap()));
                        }
                    }
                }
            }
        }
    }

    pub async fn write_block_index(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::WriteBlockIndex(key, value)).await {
            panic!("Failed to send Write Block Index command to store: {}", e);
        }
    }
    pub async fn write_block(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::WriteBlock(key, value)).await {
            panic!("Failed to send Write Block command to store: {}", e);
        }
    }
    pub async fn write_tx(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::WriteTransaction(key, value)).await {
            panic!("Failed to send Write Transaction command to store: {}", e);
        }
    }
    pub async fn write_round(&mut self, value: u64) {
        if let Err(e) = self.channel.send(StoreCommand::WriteRound(value)).await {
            panic!("Failed to send Write Round command to store: {}", e);
        }
    }
    pub async fn write_last_voted_round(&mut self, value: u64) {
        if let Err(e) = self.channel.send(StoreCommand::WriteLastVotedRound(value)).await {
            panic!("Failed to send Write LastVotedRound command to store: {}", e);
        }
    }
    pub async fn write_last_committed_round(&mut self, value: u64) {
        if let Err(e) = self.channel.send(StoreCommand::WriteLastCommittedRound(value)).await {
            panic!("Failed to send Write LastCommittedRound command to store: {}", e);
        }
    }
    pub async fn write_qc(&mut self, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::WriteQC(value)).await {
            panic!("Failed to send Write QC command to store: {}", e);
        }
    }
    pub async fn write_utxo_cache(&mut self, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::WriteUTXOCache(value)).await {
            panic!("Failed to send Write UTXO cache command to store: {}", e);
        }
    }
    pub async fn read_block_index(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadBlockIndex(key, sender)).await {
            panic!("Failed to send Read Block Index command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Block Index command from store")
    }
    pub async fn read_block(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadBlock(key, sender)).await {
            panic!("Failed to send Read Block command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Block command from store")
    }
    pub async fn notify_read_block(&mut self, key: Key) -> StoreResult<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyReadBlock(key, sender))
            .await
        {
            panic!("Failed to send NotifyReadBlock command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyReadBlock command from store")
    }
    pub async fn read_tx(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadTransaction(key, sender)).await {
            panic!("Failed to send Read Transaction command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Transaction command from store")
    }
    pub async fn notify_read_tx(&mut self, key: Key) -> StoreResult<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyReadTransaction(key, sender))
            .await
        {
            panic!("Failed to send NotifyReadTransaction command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyReadTransaction command from store")
    }
    pub async fn read_round(&mut self) -> StoreResult<Option<u64>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadRound(sender)).await {
            panic!("Failed to send Read Round command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Round command from store")
            .map(|opt| opt.map(|bytes| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }))
    }
    pub async fn read_last_voted_round(&mut self) -> StoreResult<Option<u64>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadLastVotedRound(sender)).await {
            panic!("Failed to send Read LastVotedRound command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Round command from store")
            .map(|opt| opt.map(|bytes| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }))
    }
    pub async fn read_last_committed_round(&mut self) -> StoreResult<Option<u64>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadLastCommittedRound(sender)).await {
            panic!("Failed to send Read LastCommittedRound command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Round command from store")
            .map(|opt| opt.map(|bytes| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }))
    }
    pub async fn read_qc(&mut self) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadQC(sender)).await {
            panic!("Failed to send Read QC command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read QC command from store")
    }
    pub async fn get_utxo_cache(&mut self) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::ReadUTXOCache(sender)).await {
            panic!("Failed to send Read UTXO cache command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read UTXO cache command from store")
    }
}
