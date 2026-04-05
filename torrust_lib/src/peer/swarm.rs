use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;

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
    piece_to_peers: HashMap<u32, HashSet<PeerAddr>>,
    peers_state: HashMap<PeerAddr, PeerState>,
    peers_rx: mpsc::Receiver<Vec<PeerAddr>>,
    peers_cmd: HashMap<PeerAddr, mpsc::Sender<PeerCommand>>,
    peer_events_rx: mpsc::Receiver<PeerEvent>,
    peer_events_tx: mpsc::Sender<PeerEvent>,
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
            piece_to_peers: HashMap::new(),
            peers_state: HashMap::new(),
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
        loop {
            tokio::select! {
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
                println!("connected {p:#?}");

                self.peers_state.insert(
                    p,
                    PeerState {
                        am_choking: true,
                        am_interested: false,
                        peer_choking: true,
                        peer_interested: false,
                        bitfield: Bitfield::new(0),
                    },
                );

                // Directly send interrested to be unchocked
                if let Some(cmd) = self.peers_cmd.get_mut(&p) {
                    let _ =
                        cmd.try_send(PeerCommand::Send(Message::Interested));
                }
            },
            PeerEvent::Disconnected(p) => {
                self.peers_state.remove(&p);
            },
            PeerEvent::Message(p, msg) => {
                let state = match self.peers_state.get_mut(&p) {
                    Some(s) => s,
                    None => return,
                };

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
                        println!(
                            "got piece: {index} [{begin}:] {:#?}",
                            block.len()
                        );

                        // let _ = self
                        //     .piece_manager
                        //     .write_block(index, begin, block.as_ref())
                        //     .await;
                    },

                    // Unchoke
                    Message::Unchoke => {
                        // pick a missing piece that the peer has
                        for (piece_index, peers) in &self.piece_to_peers {
                            if !self.piece_manager.has_piece(*piece_index)
                                && peers.contains(&p)
                            {
                                // request first missing block
                                for block in self
                                    .piece_manager
                                    .missing_blocks(*piece_index)
                                {
                                    let _ = self.peers_cmd.get(&p).map(|tx| {
                                        let _ = tx.try_send(PeerCommand::Send(
                                            Message::Request {
                                                index: *piece_index,
                                                begin: block.begin,
                                                length: block.length,
                                            },
                                        ));
                                    });
                                    break;
                                }
                                break;
                            }
                        }
                    },

                    // peer stops allowing requests
                    Message::Choke => {
                        // if let Some(cmd) = self.peers_cmd.get(&p) {
                        // let _ = cmd.try_send(PeerCommand::Se);
                        // }
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
