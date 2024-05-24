use crate::database::Database;
use clap::Parser;
use firewood::Firewood;
use runner::Runner;

mod database;
mod firewood;
pub mod runner;
mod utils;

#[derive(Parser, Debug)]
#[command(name = "bspt", version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    engine: String,

    #[arg(short, long, default_value = "./dataset")]
    datadir: String,

    #[arg(short = 'b', long = "bs", default_value_t = 3000)]
    batch_size: u32,

    #[arg(short = 'j', long = "numjobs", default_value_t = 10)]
    num_jobs: usize,
}

#[tokio::main]
async fn main() {
    // parse the command args
    let args = Args::parse();

    // init database by specify engine
    match args.engine.as_str() {
        "firewood" => {
            match Firewood::open(args.datadir.clone()).await {
                Ok(db) => run(db, args).await,
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
    let runner = Runner::new(db, args.batch_size, args.num_jobs, 10000, 300, 300);
    runner.run().await;
}
