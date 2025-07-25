use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use mempool::TransactionFields;
use placeholder_project_name_placeholder_zk::field::goldilocks_field::GoldilocksField;
use placeholder_project_name_placeholder_zk::field::types::Field;
use placeholder_project_name_placeholder_zk::placeholder_project_name_placeholder_patch::{PlaceholderProjectNamePlaceholderField, PlaceholderProjectNamePlaceholderHash, PlaceholderProjectNamePlaceholderProof, PlaceholderProjectNamePlaceholderVerifierOnlyCircuitData};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use l0::Transaction;

#[derive(Parser)]
#[clap(
    author,
    version,
    about,
    long_about = "Benchmark client for HotStuff nodes."
)]
struct Cli {
    /// The network address of the node where to send txs.
    #[clap(value_parser, value_name = "ADDR")]
    target: SocketAddr,
    /// The nodes timeout value.
    #[clap(short, long, value_parser, value_name = "INT")]
    timeout: u64,
    /// The size of each transaction in bytes.
    #[clap(short, long, value_parser, value_name = "INT")]
    size: usize,
    /// The rate (txs/s) at which to send the transactions.
    #[clap(short, long, value_parser, value_name = "INT")]
    rate: u64,
    /// Network addresses that must be reachable before starting the benchmark.
    #[clap(short, long, value_parser, value_name = "[Addr]", multiple = true)]
    nodes: Vec<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("Node address: {}", cli.target);
    info!("Transactions size: {} B", cli.size);
    info!("Transactions rate: {} tx/s", cli.rate);
    let client = Client {
        target: cli.target,
        size: cli.size,
        rate: cli.rate,
        timeout: cli.timeout,
        nodes: cli.nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    timeout: u64,
    nodes: Vec<SocketAddr>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 1; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION; //1s

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 16 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut r: u64 = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                let tx = Transaction{
                    proof: PlaceholderProjectNamePlaceholderProof::from([GoldilocksField::ZERO; 16581]),
                    snap: PlaceholderProjectNamePlaceholderHash::default(),
                    from: PlaceholderProjectNamePlaceholderVerifierOnlyCircuitData::from([GoldilocksField::ZERO; 68]),
                    to: PlaceholderProjectNamePlaceholderHash::default(),
                    nonce: PlaceholderProjectNamePlaceholderField::from(r),
                    amount: PlaceholderProjectNamePlaceholderField::default(),
                    gas: PlaceholderProjectNamePlaceholderField::default(),
                    payload: vec![PlaceholderProjectNamePlaceholderField::default(); 8]};
                r = r + 1;
                info!("Sending transaction {}, {}", x, r);
                let tf = TransactionFields(tx.into());
                if let Err(e) = transport.send(Bytes::from(bincode::serialize(&tf).unwrap())).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // First wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;

        // Then wait for the nodes to be synchronized.
        info!("Waiting for all nodes to be synchronized...");
        sleep(Duration::from_millis(2 * self.timeout)).await;
    }
}
