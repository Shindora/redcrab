//! redcrab: a single-node broker speaking the Apache Kafka wire protocol.

mod api;
pub mod broker;
mod group;
mod server;
pub mod storage;

pub use broker::{Broker, BrokerConfig};
pub use server::serve;
