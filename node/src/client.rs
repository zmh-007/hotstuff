use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use crypto::Digest;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use zk::{AsBytes, Fr, ToHash};
use std::convert::TryInto;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use l0::{Out, Tx, Wp};

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

            for _ in 0..burst {
                let tx = Tx{
                    ix: Fr::from(r),
                    iy: Fr::from(r+1),
                    ox: Out {
                        amount: Fr::from(r),
                        owner: Fr::from(r),
                        data: vec![Fr::from(r); self.size / 32],
                    },
                    oy: Out {
                        amount: Fr::from(r),
                        owner: Fr::from(r),
                        data: vec![Fr::from(r); self.size / 32],
                    },
                };

                let wp = Wp {
                    vk: dummy_vk(),
                    proof: dummy_proof (),
                    val: tx,
                };
                r = r + 1;
                let b = wp.val.clone().hash().enc().collect::<Vec<u8>>();
                info!("Sending transaction {:?}", Digest(b.try_into().unwrap()));
                let tx_bytes: Vec<u8> = wp.enc().collect();
                if let Err(e) = transport.send(Bytes::from(tx_bytes)).await {
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

pub fn dummy_vk() -> zk::Vk {
    zk::Vk::dec(&mut hex::decode("91e33e9340aa7e3eb785c21a2baea3066397ca7d3cd792d498dc10cc61a55c5d86d07e40b1b49a0a622297a312a2c90496556736ca9a7284431ea946c9b7f822dd6b05464add282f6a5358dda53fb65d956d531c1d83997fa66933d4740cfbbba48736b143fec6e419a41727d0f1d2b93a82029105864eee3ccc68ab2229491322b422bba12c90fb9357df63798593dd939d715247532fd95ee373020e69047a759c9786340e23ba430595235f87974414a83ce2843abc043918b67439d876e8c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b0cbe536a0026894debe231c3764f33d963d5f741410060da108cd1f46cdb11a6875b2419d836dfd3f0cb0f960523db40000000103").unwrap().into_iter()).unwrap()
}

pub fn dummy_proof() -> zk::Proof {
    zk::Proof::dec(&mut hex::decode("a68b9a3909a5ecca9e4e062981e2bd7a32a93c7f191f2239af08aa272e907c194f24aca73025c4eab68be3616843274ea97959687760defcbabe4bb1fc4ea02966affc571fd3cdc755be9910ad199a98ac88e74b809155443890a2e2e3873e4392eb207f2fda35ae8da1a1611b96ddb57c67dec9208aaee9b4fcfce175bf1481e598b7fc11fb0cf4d69bd7b011d9db43aae2cf3257a00a0c43bc967e2330683e141004b10b0e4758eca07f1c935b1938feba5e2060c7bd009996a8ce849a8d82af0b143824991705261bdce4726090e87b0000855b563758f15f909e19dd813cdcb490b447c516a0cb08abf1ac3751d78c37d51a82ad4a588d861b011840437a0199fafffd189227b52a6d5e5453a49ed9a72dbe19d98601966da423bc028d1c96892502ea55cd03359f93299206a914da4d7ef57ac0d9356498c8359ad5c1d6e0bcf5e5ed63b40036431ef31c4c5a0f8d483d88cd56c77249ad2eb5223afdd56066163bd8d76aef8ef9062720f9fb17c208bb0d1582e986c68449c63e51fcc4ab3cfdee034a2ccb1c6213dedf546f595c5f0e75cf62c753cb0a518d64730f6c8e0a13df4843716c802e82225e1359d0a04ab3968ffd7e7caca17deaba65a0806bd5f3efbd84c1667f5b9dea9bff19b5f0cbf4c79696d9a899a8ef98d548b90e14828a226bbb693544b11ca3dcc73447d2aa3667244a339d6ec78d58a33aa1806ce8d9001c1e59a7a438024171d2757afba407ab14dcc832b2cc4be6cc5fb0085839795e55217bc29f8e93c4abaac7d121275c0a5d70a54450434a743c919dcb0fd3d1bbc19a0a7375adc18c8abfc39fca72afffb82dc4e4ad30a4d36dc2692f0e3c6322a45200f679634138142111402278d5fa9dbcd1654589db7ae86561bb46471ba2baed85a275ca01bf70a2b11e8febf418198f6725145fe2f9b60ec5610491fd5149f2d04292466149c8c9f9b01e7104a353996f3fd2fe3ea4c9bf503c05b8f7af376f466e080ec3e90bbc01942e26eaa39a648b25b150d0087a7c2912").unwrap().into_iter()).unwrap()
}
