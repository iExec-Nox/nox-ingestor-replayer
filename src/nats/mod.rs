//! NATS client module for publishing events to JetStream

mod client;
mod publisher;

pub use client::NatsClient;
pub use publisher::{PublishOutcome, Publisher};
