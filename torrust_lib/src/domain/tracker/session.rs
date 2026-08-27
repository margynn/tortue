use std::net::SocketAddr;
use std::time::Duration;

use super::{
    AnnounceEvent, AnnounceRequest, Node, SessionStats, TrackerResponse,
};
use crate::domain::torrent::InfoHash;

pub(crate) enum Input {
    TimerFired,
    AnnounceSucceeded(TrackerResponse),
    AnnounceFailed,
    Stop,
}

pub(crate) enum Output {
    Announce(AnnounceRequest),
    ScheduleAnnounce(Duration),
    EmitPeers(Vec<SocketAddr>),
    Stop,
}

pub(crate) struct TrackerSession {
    info_hash: InfoHash,
    node: Node,
    next_event: Option<AnnounceEvent>,
    backoff: Duration,
    stats: SessionStats,
}

impl TrackerSession {
    const INITIAL_BACKOFF: Duration = Duration::from_secs(15);
    const MAX_BACKOFF: Duration = Duration::from_secs(3600);

    pub(crate) fn new(
        info_hash: InfoHash,
        node: Node,
        content_size: u64,
    ) -> (Self, Vec<Output>) {
        let session = Self {
            info_hash,
            node,
            next_event: Some(AnnounceEvent::Started),
            backoff: Self::INITIAL_BACKOFF,
            stats: SessionStats {
                uploaded: 0,
                downloaded: 0,
                left: content_size,
            },
        };
        (session, vec![Output::ScheduleAnnounce(Duration::ZERO)])
    }

    pub(crate) fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::TimerFired => self.on_timer_fired(),
            Input::AnnounceSucceeded(r) => self.on_announce_succeeded(r),
            Input::AnnounceFailed => self.on_announce_failed(),
            Input::Stop => vec![Output::Stop],
        }
    }

    fn on_timer_fired(&mut self) -> Vec<Output> {
        let request = AnnounceRequest {
            info_hash: self.info_hash,
            peer_id: self.node.id,
            port: self.node.port,
            stats: SessionStats {
                uploaded: self.stats.uploaded,
                downloaded: self.stats.downloaded,
                left: self.stats.left,
            },
            event: self.next_event.take().unwrap_or(AnnounceEvent::None),
            compact: true,
        };
        vec![Output::Announce(request)]
    }

    fn on_announce_succeeded(&mut self, resp: TrackerResponse) -> Vec<Output> {
        self.backoff = Duration::from_secs(30);
        let interval = resp.interval.max(60);
        let peers = resp.peers.clone();
        vec![
            Output::ScheduleAnnounce(Duration::from_secs(interval as u64)),
            Output::EmitPeers(peers),
        ]
    }

    fn on_announce_failed(&mut self) -> Vec<Output> {
        let current_backoff = self.backoff;
        self.backoff = (self.backoff * 2).min(Self::MAX_BACKOFF);
        vec![Output::ScheduleAnnounce(current_backoff)]
    }
}
