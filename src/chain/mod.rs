mod client;
mod parser;
mod reader;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;

pub use client::ChainClient;
pub use parser::NoxEventParser;
pub use reader::{BatchResult, BlockReader};

use crate::nats::Publisher;
use crate::replay::ReplayJobStatus;

/// Per-chain resources: the reader/publisher pair, a single-job slot lock, and
/// the status/task handle for whatever replay is (or was) running on this chain.
pub struct ChainPipeline {
    pub reader: Arc<BlockReader>,
    pub publisher: Arc<Publisher>,
    /// One permit — at most one replay job per chain at a time.
    pub lock: Arc<Semaphore>,
    pub job_status: Arc<RwLock<ReplayJobStatus>>,
    pub replay_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl ChainPipeline {
    pub fn new(reader: BlockReader, publisher: Publisher) -> Self {
        Self {
            reader: Arc::new(reader),
            publisher: Arc::new(publisher),
            lock: Arc::new(Semaphore::new(1)),
            job_status: Arc::new(RwLock::new(ReplayJobStatus::default())),
            replay_task: Arc::new(Mutex::new(None)),
        }
    }
}

/// All configured chains plus a global cap on concurrently-running replay jobs.
pub struct ChainRegistry {
    pub pipelines: HashMap<u32, ChainPipeline>,
    pub global: Arc<Semaphore>,
}

impl ChainRegistry {
    pub fn new(pipelines: HashMap<u32, ChainPipeline>, max_concurrent_chains: usize) -> Self {
        Self {
            pipelines,
            global: Arc::new(Semaphore::new(max_concurrent_chains)),
        }
    }

    pub fn get(&self, chain_id: u32) -> Option<&ChainPipeline> {
        self.pipelines.get(&chain_id)
    }
}
