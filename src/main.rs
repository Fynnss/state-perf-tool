use crate::database::Database;
use clap::Parser;
use eth_trie::MemoryMPT;
use firewood::Firewood;
use runner::Runner;

mod database;
mod eth_trie;
mod firewood;
pub mod runner;
mod stat;
mod utils;

#[derive(Parser, Debug)]
#[command(name = "bsperftool", version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    engine: String,

    #[arg(short, long, default_value = "./dataset")]
    datadir: String,

    #[arg(short = 'b', long = "bs", default_value_t = 3000)]
    batch_size: u32,

    #[arg(short = 'j', long = "num_jobs", default_value_t = 10)]
    num_jobs: usize,

    #[arg(short = 'r', long = "key_range", default_value_t = 100000000)]
    key_range: u64,

    #[arg(short = 'm', long = "min_value_size", default_value_t = 300)]
    min_value_size: u32,

    #[arg(short = 'M', long = "max_value_size", default_value_t = 300)]
    max_value_size: u32,

    #[arg(short = 'd', long = "delete_ratio", default_value_t = 0.2)]
    delete_ratio: f64,
}

#[tokio::main]
async fn main() {
    // parse the command args
    let args = Args::parse();

    // init database by specify engine
    match args.engine.as_str() {
        "firewood" => {
            match Firewood::open(args.datadir.clone()).await {
                Ok(db) => {
                    println!("Open firewood success");
                    run(db, args).await;
                }
                Err(e) => println!("Failed to open firewood database: {}", e),
            };
        }
        "memorydb" => {
            match MemoryMPT::open(args.datadir.clone()).await {
                Ok(db) => {
                    println!("Open memory MPT success");
                    run(db, args).await;
                }
                Err(e) => println!("Failed to open firewood database: {}", e),
            };
        }
        _ => {
            println!("unsupported engine. Please check.")
        }
    }
}

async fn run<T>(db: T, args: Args)
where
    T: Database + Send + Sync + 'static,
{
    let runner = Runner::new(
        db,
        args.batch_size,
        args.num_jobs,
        args.key_range,
        args.min_value_size,
        args.max_value_size,
        args.delete_ratio,
    );
    runner.run().await;
}
