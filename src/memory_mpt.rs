use std::sync::Arc;

use crate::database::Database;
use async_trait::async_trait;
use eth_trie::{EthTrie, MemoryDB, Trie};
use tokio::sync::RwLock;

pub struct MemoryMPT {
    eth_trie: Arc<RwLock<EthTrie<MemoryDB>>>,
}

#[async_trait]
impl Database for MemoryMPT {
    async fn open(_: String) -> Result<Self, String> {
        let memdb = Arc::new(MemoryDB::new(true));
        let trie = Arc::new(RwLock::new(EthTrie::new(memdb.clone())));
        Ok(MemoryMPT { eth_trie: trie })
    }

    async fn get(&self, key: [u8; 32]) -> Result<Option<Vec<u8>>, String> {
        let trie = self.eth_trie.read().await;

        match trie.get(&key) {
            Ok(val) => Ok(val),
            Err(e) => return Err(format!("failed to get from trie: {}", e)),
        }
    }

    async fn put(&self, key: [u8; 32], val: Vec<u8>) {
        let mut trie = self.eth_trie.write().await;

        if let Err(e) = trie.insert(&key, &val) {
            println!("failed to put into trie: {}", e)
        }
    }

    async fn delete(&self, key: [u8; 32]) {
        let mut trie = self.eth_trie.write().await;
        if let Err(e) = trie.remove(&key) {
            println!("failed to put into trie: {}", e)
        }
    }

    async fn commit(&self) -> Result<(), String> {
        let mut trie = self.eth_trie.write().await;
        match trie.root_hash() {
            Ok(_) => Ok(()),
            Err(e) => return Err(format!("failed to commit: {}", e)),
        }
    }
}
