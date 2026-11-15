//! redcrab: a broker speaking the Apache Kafka wire protocol, replicating every partition and its
//! cluster metadata with Raft across a static set of nodes.

mod api;
pub mod broker;
pub mod cluster;
mod group;
pub mod raft;
pub mod replica;
mod server;
pub mod storage;
mod transport;

pub use broker::{Broker, BrokerConfig};
pub use cluster::{Peer, StartError};
pub use server::serve;
