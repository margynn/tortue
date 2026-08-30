# Architecture

## Principe

Deux couches strictes :

- **`domain/`** — logique pure, sans I/O, sans tokio. State machines, parsing, protocoles.
- **`adapters/`** — I/O tokio. Chaque adapter pilote un state machine du domain dans une tâche async.

Les adapters se parlent exclusivement via des **channels `mpsc`**. Aucun adapter ne référence un autre directement.

---

## Domain

### Entités principales

| Module | Rôle |
|---|---|
| `domain::torrent` | `Metainfo` — contenu statique d'un fichier .torrent |
| `domain::tracker` | `TrackerSession` — state machine d'announce (backoff, intervalles) |
| `domain::tracker::http` | Encoding/parsing HTTP announce (pur) |
| `domain::tracker::udp` | Encoding/parsing UDP tracker protocol (pur) |
| `domain::peer` | `PeerSession` — state machine d'une connexion peer (handshake, messages) |
| `domain::peer_pool` | `PeerPool` — gère un ensemble dynamique de peers pour un torrent |
| `domain::pieces` | `PieceManager` — suivi des pièces téléchargées, sélection (rarest-first) |

### `PeerPool`

Représente l'ensemble évolutif des peers participant au téléchargement d'**un** torrent.
Des peers peuvent apparaître (découverts par trackers, DHT, PEX) ou disparaître à tout moment.

```
Input:
  PeersDiscovered(Vec<SocketAddr>)      // depuis tracker, DHT, PEX...
  PeerConnected { addr, peer_id, bitfield }
  PeerDisconnected(addr)
  PieceAvailable { from: addr, index }
  PieceDownloaded(index)
  Stop

Output:
  ConnectPeer(SocketAddr)
  DisconnectPeer(SocketAddr)
  RequestPiece { from: SocketAddr, index: usize }
  Completed
```

---

## Adapters

### Acteurs (tâches tokio)

| Adapter | Pilote | Canal principal |
|---|---|---|
| `tracker_task` | `TrackerSession` | → `peers_tx: Sender<Vec<SocketAddr>>` |
| `peer_pool_task` | `PeerPool` | ← peers, → commandes peers |
| `peer_io` | `PeerSession` | ← commandes, → pièces reçues |
| `storage_task` | `TokioStorage` | ← pièces à écrire |

### `TorrentRunner` — l'orchestrateur

Seul endroit qui connaît la topologie complète. Câble les channels et spawn les tâches.

```rust
pub async fn run(metainfo: Metainfo, node: Node) {
    let (peers_tx, peers_rx) = mpsc::channel(128);
    let (piece_tx, piece_rx) = mpsc::channel(32);

    // Un task par endpoint tracker (HTTP ou UDP)
    for url in &metainfo.announce {
        let Ok(client) = TokioTrackerClient::new(url) else { continue };
        tokio::spawn(tracker_task(client, metainfo.clone(), node, peers_tx.clone()));
    }

    // PeerPool — reçoit les peers découverts, orchestre les téléchargements
    tokio::spawn(peer_pool_task(metainfo.clone(), peers_rx, piece_tx));

    // Storage
    tokio::spawn(storage_task(metainfo.clone(), piece_rx));
}
```

### Flux de données

```
metainfo.announce[0..N]
    → tracker_task × N  ──┐
                           │ Vec<SocketAddr>
DHT (futur)            ───┤
PEX (futur)            ───┤
                           ▼
                    peer_pool_task
                    (PeerPool domain)
                           │ ConnectPeer / RequestPiece / Disconnect
                           ▼
                    peer_io × N
                    (PeerSession domain)
                           │ pièce téléchargée
                           ▼
                    storage_task
```

---

## Extension future

Ajouter une source de peers (DHT, PEX) = brancher un nouveau `Sender<Vec<SocketAddr>>` sur le même channel `peers_rx` du `peer_pool_task`. Le domain `PeerPool` n'a pas à changer.
