use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::pieces::BlockRef;

pub(super) struct BlockAssignments {
    by_peer: HashMap<SocketAddr, HashMap<BlockRef, Instant>>,
}

impl BlockAssignments {
    const MAX_IN_FLIGHT_PER_PEER: usize = 32;
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    pub(super) fn new() -> Self {
        Self {
            by_peer: HashMap::new(),
        }
    }

    pub(super) fn assign(&mut self, b: BlockRef, addr: SocketAddr) {
        self.by_peer
            .entry(addr)
            .or_default()
            .insert(b, Instant::now());
    }

    pub(super) fn unassign(&mut self, b: BlockRef, addr: SocketAddr) {
        if let Some(blocks) = self.by_peer.get_mut(&addr) {
            blocks.remove(&b);
        }
    }

    pub(super) fn is_holder(&self, b: BlockRef, addr: SocketAddr) -> bool {
        self.by_peer
            .get(&addr)
            .is_some_and(|blocks| blocks.contains_key(&b))
    }

    pub(super) fn holder_count(&self, b: BlockRef) -> usize {
        self.by_peer
            .values()
            .filter(|blocks| blocks.contains_key(&b))
            .count()
    }

    pub(super) fn has_holder(&self, b: BlockRef) -> bool {
        self.by_peer.values().any(|blocks| blocks.contains_key(&b))
    }

    fn in_flight_for(&self, addr: SocketAddr) -> usize {
        self.by_peer.get(&addr).map_or(0, |blocks| blocks.len())
    }

    pub(super) fn release_peer(&mut self, addr: SocketAddr) -> Vec<BlockRef> {
        self.by_peer
            .remove(&addr)
            .map(|blocks| blocks.into_keys().collect())
            .unwrap_or_default()
    }

    /// Drops requests we have waited too long for, freeing the peer's slot and
    /// letting the block be offered around again — including back to that peer.
    pub(super) fn release_expired(&mut self) -> Vec<BlockRef> {
        let now = Instant::now();
        let mut expired = vec![];
        for blocks in self.by_peer.values_mut() {
            blocks.retain(|block_ref, at| {
                let alive = now < *at + Self::REQUEST_TIMEOUT;
                if !alive {
                    expired.push(*block_ref);
                }
                alive
            });
        }
        expired
    }

    pub(super) fn free_slots_for(&self, addr: SocketAddr) -> usize {
        Self::MAX_IN_FLIGHT_PER_PEER.saturating_sub(self.in_flight_for(addr))
    }

    pub(super) fn has_capacity(&self, addr: SocketAddr) -> bool {
        self.free_slots_for(addr) > 0
    }

    /// Counts requests, not distinct blocks: endgame duplicates make the two
    /// differ.
    pub(super) fn requests_in_flight(&self) -> usize {
        self.by_peer.values().map(|blocks| blocks.len()).sum()
    }
}
