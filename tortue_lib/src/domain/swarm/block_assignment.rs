use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::piece_manager::BlockRef;

pub(super) struct BlockAssignments {
    by_peer: HashMap<SocketAddr, HashMap<BlockRef, Instant>>,
}

impl BlockAssignments {
    const MAX_IN_FLIGHT_PER_PEER: usize = 32;

    /// How long a request occupies a peer's scheduling slot before we treat
    /// it as stuck: the block becomes assignable again — to another peer, or
    /// back to the same one — without waiting for this request to resolve.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// How long we still trust a `Piece` reply for a request after its slot
    /// was freed. A peer that only answers slowly still gets its data
    /// accepted within this window instead of it being discarded as
    /// unsolicited.
    const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

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

    fn requested_at(&self, b: BlockRef, addr: SocketAddr) -> Option<Instant> {
        self.by_peer.get(&addr)?.get(&b).copied()
    }

    /// Still within the scheduling window: used to avoid double-assigning a
    /// block to a peer already working on it.
    pub(super) fn is_holder(&self, b: BlockRef, addr: SocketAddr) -> bool {
        self.requested_at(b, addr)
            .is_some_and(|at| Instant::now() < at + Self::REQUEST_TIMEOUT)
    }

    /// Wider than `is_holder`: still within the grace window where a `Piece`
    /// reply from this peer for this block should be accepted, even though
    /// its scheduling slot was already freed and possibly reassigned.
    pub(super) fn accepts_from(&self, b: BlockRef, addr: SocketAddr) -> bool {
        self.requested_at(b, addr)
            .is_some_and(|at| Instant::now() < at + Self::ACCEPT_TIMEOUT)
    }

    /// One pass over every in-flight request: how many peers currently hold
    /// each block. Replaces a per-block, per-depth scan. Only counts requests
    /// still within the scheduling window — a stalled one shouldn't stop the
    /// block being handed to someone else.
    pub(super) fn holder_counts(&self) -> HashMap<BlockRef, usize> {
        let now = Instant::now();
        let mut counts = HashMap::new();
        for blocks in self.by_peer.values() {
            for (block_ref, at) in blocks {
                if now < *at + Self::REQUEST_TIMEOUT {
                    *counts.entry(*block_ref).or_insert(0) += 1;
                }
            }
        }
        counts
    }

    pub(super) fn release_peer(&mut self, addr: SocketAddr) {
        self.by_peer.remove(&addr);
    }

    /// Garbage-collects requests we will no longer accept a reply for at all,
    /// bounding memory. Freeing a request's *scheduling* slot happens well
    /// before this — see `is_holder`/`holder_counts`/`free_slots_for`.
    pub(super) fn release_expired(&mut self) {
        let now = Instant::now();
        for blocks in self.by_peer.values_mut() {
            blocks.retain(|_, at| now < *at + Self::ACCEPT_TIMEOUT);
        }
    }

    pub(super) fn free_slots_for(&self, addr: SocketAddr) -> usize {
        let now = Instant::now();
        let in_flight = self.by_peer.get(&addr).map_or(0, |blocks| {
            blocks
                .values()
                .filter(|at| now < **at + Self::REQUEST_TIMEOUT)
                .count()
        });
        Self::MAX_IN_FLIGHT_PER_PEER.saturating_sub(in_flight)
    }

    /// Counts requests still within the scheduling window, not distinct
    /// blocks: endgame duplicates make the two differ.
    pub(super) fn requests_in_flight(&self) -> usize {
        let now = Instant::now();
        self.by_peer
            .values()
            .map(|blocks| {
                blocks
                    .values()
                    .filter(|at| now < **at + Self::REQUEST_TIMEOUT)
                    .count()
            })
            .sum()
    }
}
