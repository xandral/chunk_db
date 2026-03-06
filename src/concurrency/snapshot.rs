#[derive(Debug, Clone)]
pub struct Snapshot {
    pub tx_id: u64,
}

impl Snapshot {
    pub fn new(tx_id: u64) -> Self {
        Self { tx_id }
    }
}
