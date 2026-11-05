use bytes::Bytes;

pub trait StateMachine: Send + Sync {
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes>;
}
