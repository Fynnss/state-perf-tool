use chrono::{SecondsFormat, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use futures::future::join_all;

use crate::database::Database;
use crate::stat::Stat;
use crate::utils::{generate_key, generate_value};

pub struct Runner<T>
where
    T: Database + Send + Sync,
{
    db: Arc<T>,
    batch_size: u32,
    num_jobs: usize,
    key_range: u64,
    min_value_size: u32,
    max_value_size: u32,
    stat: Arc<Stat>,
    last_stat_time: Mutex<Instant>,
}

impl<T> Runner<T>
where
    T: Database + Send + Sync + 'static,
{
    pub fn new(
        db: T,
        batch_size: u32,
        num_jobs: usize,
        key_range: u64,
        min_value_size: u32,
        max_value_size: u32,
    ) -> Self {
        Self {
            db: Arc::new(db),
            batch_size,
            num_jobs,
            key_range,
            min_value_size,
            max_value_size,
            stat: Arc::new(Stat::new()),
            last_stat_time: Mutex::new(Instant::now()),
        }
    }

    pub async fn run(&self) {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        // Listen for Ctrl+C signal
        ctrlc::set_handler(move || {
            running_clone.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        // Execute the function in an infinite loop until Ctrl+C signal is received
        while running.load(Ordering::SeqCst) {
            self.run_internal().await;
        }
    }

    async fn run_internal(&self) {
        let rw_start = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.num_jobs));
        // get and put
        let key_range = self.key_range;
        let min_value_size = self.min_value_size;
        let max_value_size = self.max_value_size;
        let tasks = (0..self.batch_size)
            .map(|_| {
                let db = Arc::clone(&self.db);
                let stat = Arc::clone(&self.stat);
                let permit = semaphore.clone().acquire_owned();
                tokio::spawn(async move {
                    task(db, key_range, min_value_size, max_value_size, stat).await;
                    drop(permit);
                })
            })
            .collect::<Vec<_>>();

        join_all(tasks).await;
        let rw_duration = rw_start.elapsed();

        // commit
        let commit_start = Instant::now();
        match self.db.commit().await {
            Ok(_) => {}
            Err(e) => println!("failed to commit: {}", e),
        };
        let commit_duration = commit_start.elapsed();

        // print stat
        let delta = self.last_stat_time.lock().unwrap().elapsed();
        if delta >= Duration::new(1, 0) {
            println!(
                "[{}] Perf In Progress. {}, elapsed: [rw={:?}, commit={:?}]",
                Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                self.stat.calc_tps_and_output(delta),
                rw_duration,
                commit_duration,
            );
            // set to the systemTime::now
            let mut last_time = self.last_stat_time.lock().unwrap();
            *last_time = Instant::now();
        }
    }
}

async fn task<T>(
    db: Arc<T>,
    key_range: u64,
    min_value_size: u32,
    max_value_size: u32,
    stat: Arc<Stat>,
) where
    T: Database + Send + Sync + 'static,
{
    let range = 0..key_range;
    let key = generate_key(range);
    let db = Arc::clone(&db);
    match db.get(key).await {
        Ok(Some(val)) => {
            db.put(key, val).await;
            stat.inc_put(1);
            stat.inc_get(1);
        }
        Ok(None) => {
            let value = generate_value(min_value_size, max_value_size);
            db.put(key, value).await;
            stat.inc_put(1);
            stat.inc_get(1);
        }
        Err(e) => {
            let key_str = String::from_utf8_lossy(&key);
            println!("task get failed, key = {}, err= {}", key_str.to_string(), e)
        }
    }
}
