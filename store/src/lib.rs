use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::oneshot;
use rocksdb::{DB, Options, ColumnFamilyDescriptor, Error};

pub type StoreError = Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

const BLOCKS_CF: &str = "blocks";
const TRANSACTIONS_CF: &str = "transactions";

pub enum StoreCommand {
    WriteBlock(Key, Value),
    WriteTransaction(Key, Value),
    
    ReadBlock(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    ReadTransaction(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    
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
            ColumnFamilyDescriptor::new(BLOCKS_CF, Options::default()),
            ColumnFamilyDescriptor::new(TRANSACTIONS_CF, Options::default()),
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
            let blocks_cf = db.cf_handle(BLOCKS_CF).unwrap();
            let transactions_cf = db.cf_handle(TRANSACTIONS_CF).unwrap();

            match command {
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
                StoreCommand::ReadBlock(key, sender) => {
                    let response = db.get_cf(&blocks_cf, &key);
                    let _ = sender.send(response);
                }
                StoreCommand::ReadTransaction(key, sender) => {
                    let response = db.get_cf(&transactions_cf, &key);
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
}
