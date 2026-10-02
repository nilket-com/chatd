//! chatd: a durable local message broker between coding agents. See plans/0001.

pub mod adapter;
pub mod client;
pub mod hub;
pub mod journal;
pub mod legacy;
pub mod notify;
pub mod paths;
pub mod proto;
pub mod restore;
pub mod server;
pub mod store;
pub mod text;
