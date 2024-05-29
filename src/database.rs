use async_trait::async_trait;

#[async_trait]
pub trait Database {
    async fn open(datadir: String) -> Result<Self, String>
    where
        Self: Sized;
    async fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>, String>;
    async fn put(&self, key: &[u8; 32], value: Vec<u8>);
    async fn delete(&self, key: &[u8; 32]);
    async fn commit(&self) -> Result<(), String>;
}
