mod client;
mod parser;
mod reader;

pub use client::ChainClient;
pub use parser::NoxEventParser;
pub use reader::{BatchResult, BlockReader};

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::nats::Publisher;

pub struct ChainRegistry {
    pub pipelines: HashMap<u32, ChainPipeline>,
    pub global: Arc<Semaphore>,
}

pub struct ChainPipeline {
    pub reader: Arc<BlockReader>,
    pub publisher: Arc<Publisher>,
    pub lock: Arc<Semaphore>,
}

impl ChainRegistry {
    pub fn get(&self, chain_id: u32) -> Option<&ChainPipeline> {
        self.pipelines.get(&chain_id)
    }
}

impl ChainPipeline {
    pub fn new(reader: Arc<BlockReader>, publisher: Arc<Publisher>) -> Self {
        Self {
            reader,
            publisher,
            lock: Arc::new(Semaphore::new(1)),
        }
    }
}
