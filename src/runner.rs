use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

use futures::future::join_all;

use crate::database::Database;
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
        let semaphore = Arc::new(Semaphore::new(self.num_jobs));
        // get and put
        let key_range = self.key_range;
        let min_value_size = self.min_value_size;
        let max_value_size = self.max_value_size;
        let db = Arc::clone(&self.db);
        let tasks = (0..self.batch_size)
            .map(|_| {
                let semaphore = semaphore.clone();
                let db = Arc::clone(&db);
                tokio::spawn(async move {
                    let permit = semaphore.acquire_owned().await.unwrap();
                    task(db, key_range, min_value_size, max_value_size).await;
                    drop(permit);
                })
            })
            .collect::<Vec<_>>();

        join_all(tasks).await;

        // commit
        match db.commit().await {
            Ok(_) => {}
            Err(e) => println!("failed to commit: {}", e),
        };
    }
}

async fn task<T>(db: Arc<T>, key_range: u64, min_value_size: u32, max_value_size: u32)
where
    T: Database + Send + Sync + 'static,
{
    let range = 0..key_range;
    let key = generate_key(range);
    let db = Arc::clone(&db);
    match db.get(key).await {
        Ok(Some(val)) => {
            db.put(key, val).await;
        }
        Ok(None) => {
            let value = generate_value(min_value_size, max_value_size);
            db.put(key, value).await;
        }
        Err(e) => {
            let key_str = String::from_utf8_lossy(&key);
            println!("task get failed, key = {}, err= {}", key_str, e)
        }
    }
}
