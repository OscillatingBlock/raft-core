use bytes::Bytes;
use std::{collections::HashMap, sync::RwLock};
use tracing::{debug, info, instrument};

pub trait StateMachine: Send + Sync {
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes>;
}
