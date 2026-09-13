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

    /// One pass over every in-flight request: how many peers currently hold
    /// each block. Replaces a per-block, per-depth scan.
    pub(super) fn holder_counts(&self) -> HashMap<BlockRef, usize> {
        let mut counts = HashMap::new();
        for blocks in self.by_peer.values() {
            for block_ref in blocks.keys() {
                *counts.entry(*block_ref).or_insert(0) += 1;
            }
        }
        counts
    }

    pub(super) fn release_peer(&mut self, addr: SocketAddr) {
        self.by_peer.remove(&addr);
    }

    /// Drops requests we have waited too long for, freeing the peer's slot and
    /// letting the block be offered around again — including back to that peer.
    pub(super) fn release_expired(&mut self) {
        let now = Instant::now();
        for blocks in self.by_peer.values_mut() {
            blocks.retain(|_, at| now < *at + Self::REQUEST_TIMEOUT);
        }
    }

    pub(super) fn free_slots_for(&self, addr: SocketAddr) -> usize {
        let in_flight = self.by_peer.get(&addr).map_or(0, |blocks| blocks.len());
        Self::MAX_IN_FLIGHT_PER_PEER.saturating_sub(in_flight)
    }

    /// Counts requests, not distinct blocks: endgame duplicates make the two
    /// differ.
    pub(super) fn requests_in_flight(&self) -> usize {
        self.by_peer.values().map(|blocks| blocks.len()).sum()
    }
}
