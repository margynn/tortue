use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rand::seq::IteratorRandom;
use tokio::sync::mpsc;
use tokio::time::interval;

use super::client::PeerClient;
use crate::metainfo::Metainfo;
use crate::peer::PeerAddr;
use crate::peer::bitfield::Bitfield;
use crate::peer::client::Message;
use crate::peer::state::PeerState;
use crate::pieces::PieceManager;
use crate::tracker::session::Node;

pub struct Swarm {
    metainfo: Metainfo,
    node: Node, // todo rename
    piece_manager: PieceManager,
    peers: HashMap<PeerAddr, PeerRuntime>,
    piece_to_peers: HashMap<u32, HashSet<PeerAddr>>,
    peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    peers_cmd: HashMap<PeerAddr, mpsc::Sender<PeerCommand>>,
    peer_events_rx: mpsc::Receiver<PeerEvent>,
    peer_events_tx: mpsc::Sender<PeerEvent>,
}

struct PeerRuntime {
    state: PeerState,
    in_flight: usize,
}

#[derive(Debug)]
pub enum PeerEvent {
    Connected(PeerAddr),
    Disconnected(PeerAddr),
    Message(PeerAddr, Message),
}

#[derive(Debug)]
pub enum PeerCommand {
    Shutdown,
    Send(Message),
}

const MAX_IN_FLIGHT_PER_PEER: usize = 30;

impl Swarm {
    const PEER_CMD_CHAN_SIZE: usize = 32;
    const SWARM_EVENT_CHAN_SIZE: usize = 256;

    pub fn new(
        metainfo: Metainfo,
        piece_manager: PieceManager,
        node: Node,
        peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(Self::SWARM_EVENT_CHAN_SIZE);
        Self {
            metainfo,
            node,
            piece_manager,
            peers: HashMap::new(),
            piece_to_peers: HashMap::new(),
            peers_rx,
            peers_cmd: HashMap::new(),
            peer_events_rx: rx,
            peer_events_tx: tx,
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            self.run_swarm().await;
        });
    }

    async fn run_swarm(&mut self) {
        let mut tick = interval(Duration::from_millis(5));

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    self.request_block();
                }

                Some(peers) = self.peers_rx.recv() => {
                    for peer_addr in peers {
                        self.spawn_peer(peer_addr);
                    }
                }

                Some(event) = self.peer_events_rx.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
    }

    // TODO: should be sync latter
    async fn handle_event(&mut self, evt: PeerEvent) {
        match evt {
            PeerEvent::Connected(p) => {
                // println!("connected {p:#?}");

                self.peers.insert(
                    p,
                    PeerRuntime {
                        state: PeerState {
                            am_choking: true,
                            am_interested: false,
                            peer_choking: true,
                            peer_interested: false,
                            bitfield: Bitfield::new(0),
                        },
                        in_flight: 0,
                    },
                );

                // Directly send interrested to be unchocked
                if let Some(cmd) = self.peers_cmd.get_mut(&p) {
                    let _ =
                        cmd.try_send(PeerCommand::Send(Message::Interested));
                }
            },
            PeerEvent::Disconnected(p) => {
                self.peers.remove(&p);
            },
            PeerEvent::Message(p, msg) => {
                let peer = match self.peers.get_mut(&p) {
                    Some(s) => s,
                    None => return,
                };
                let state = &mut peer.state;

                state.apply(&msg);

                match msg {
                    // Update bitfields
                    Message::Bitfield(_) | Message::Have(_) => {
                        // remove peer from all pieces
                        for peers in self.piece_to_peers.values_mut() {
                            peers.remove(&p);
                        }
                        // re-add
                        for i in &state.bitfield {
                            let set = self
                                .piece_to_peers
                                .entry(i)
                                .or_insert(HashSet::new());
                            set.insert(p);
                        }
                    },

                    // Store the data
                    Message::Piece { index, begin, block } => {
                        let _ = self
                            .piece_manager
                            .write_block(index, begin, block.as_ref())
                            .await;
                        // println!("{r:#?}");

                        peer.in_flight = peer.in_flight.saturating_sub(1);
                    },

                    Message::Unchoke => {
                        // todo: should request blocks
                    },

                    // peer stops allowing requests
                    Message::Choke => {
                        // todo: should cancel requests
                    },

                    _ => {},
                }

                // DO NOT IMPLEMENT YET:
                // Seeder: (I am uploading data to peers)
                // Interrested -> send unchoke
                // NotInterrested -> send chocke
                // Request -> send the piece
            },
        }
    }

    fn request_block(&mut self) {
        let mut dispatched = 0;

        for (piece_index, peers) in &self.piece_to_peers {
            if self.piece_manager.has_piece(*piece_index) {
                continue;
            }

            let blocks: Vec<_> =
                self.piece_manager.missing_blocks(*piece_index).collect();

            for block in blocks {
                let mut rng = rand::rng();

                let peer = peers
                    .iter()
                    .filter(|p| {
                        if let Some(runtime) = self.peers.get(*p) {
                            !runtime.state.peer_choking
                                && runtime.in_flight < MAX_IN_FLIGHT_PER_PEER
                        } else {
                            false
                        }
                    })
                    .choose(&mut rng);

                let peer = match peer {
                    Some(p) => *p,
                    None => continue,
                };

                let sent = if let Some(tx) = self.peers_cmd.get(&peer) {
                    tx.try_send(PeerCommand::Send(Message::Request {
                        index: *piece_index,
                        begin: block.begin,
                        length: block.length,
                    }))
                    .is_ok()
                } else {
                    false
                };

                if sent {
                    // mark as requested BEFORE incrementing (important for dedup)
                    self.piece_manager
                        .mark_block_requested(*piece_index, block.begin);

                    if let Some(runtime) = self.peers.get_mut(&peer) {
                        runtime.in_flight += 1;
                    }
                }

                if sent {
                    dispatched += 1;
                    if dispatched >= 50 {
                        return;
                    }
                }
            }
        }
    }

    fn spawn_peer(&mut self, peer_addr: PeerAddr) {
        if self.peers_cmd.contains_key(&peer_addr) {
            return;
        }

        let metainfo = self.metainfo.clone();
        let client_id = self.node.id;
        let (cmd_tx, cmd_rx) = mpsc::channel(Self::PEER_CMD_CHAN_SIZE);
        let event_tx = self.peer_events_tx.clone();
        self.peers_cmd.insert(peer_addr, cmd_tx);

        tokio::spawn(async move {
            PeerClient::new(peer_addr, client_id, metainfo)
                .run(cmd_rx, event_tx)
                .await;
        });
    }
}
