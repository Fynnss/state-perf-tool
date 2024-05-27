use std::sync::Arc;

use async_trait::async_trait;
use firewood::{
    db::{BatchOp, Db, DbConfig, Proposal},
    storage::WalConfig,
    v2::api::{Batch, Db as _, DbView},
};
use tokio::sync::Mutex;

use crate::database::Database;

pub struct Firewood {
    db: Db,
    batch: Arc<Mutex<Batch<Vec<u8>, Vec<u8>>>>,
}

#[async_trait]
impl Database for Firewood {
    async fn open(datadir: String) -> Result<Self, String> {
        let cfg = DbConfig::builder()
            .truncate(false)
            .wal(WalConfig::builder().max_revisions(100).build());

        match Db::new(datadir, &cfg.build()).await {
            Ok(db) => Ok(Firewood {
                db,
                batch: Arc::new(Mutex::new(Vec::new())),
            }),
            Err(e) => Err(format!("Failed to open database: {}", e)),
        }
    }

    async fn get(&self, key: [u8; 32]) -> Result<Option<Vec<u8>>, String> {
        let root_hash = match self.db.root_hash().await {
            Ok(hash) => hash,
            Err(e) => return Err(format!("Failed to get root hash: {}", e)),
        };

        // Empty Tree
        if hex::encode(root_hash)
            == "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
        {
            return Ok(None);
        };

        let rev = match self.db.revision(root_hash).await {
            Ok(rev) => rev,
            Err(e) => return Err(format!("Failed to get revision: {}", e)),
        };

        match rev.val(key).await {
            Ok(val) => Ok(val),
            Err(e) => Err(format!("Failed to get key: {}", e)),
        }
    }

    async fn put(&self, key: [u8; 32], val: Vec<u8>) {
        let mut vec = self.batch.lock().await;
        vec.push(BatchOp::Put {
            key: key.to_vec(),
            value: val.clone(),
        })
    }

    async fn delete(&self, key: [u8; 32]) {
        let mut vec = self.batch.lock().await;
        vec.push(BatchOp::Delete { key: key.to_vec() })
    }

    async fn commit(&self) -> Result<(), String> {
        let mut guard = self.batch.lock().await;
        let bt: Batch<Vec<u8>, Vec<u8>> = std::mem::take(&mut *guard);

        let proposal = match self.db.propose(bt).await {
            Ok(proposal) => Arc::new(proposal),
            Err(err) => return Err(err.to_string()),
        };

        proposal.commit().await.map_err(|err| err.to_string())
    }
}
