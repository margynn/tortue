# Architecture

## Principe

Deux couches strictes :

- **`domain/`** — logique pure, sans I/O, sans tokio. State machines, parsing, protocoles.
- **`adapters/`** — I/O tokio. Chaque adapter pilote un state machine du domain dans une tâche async.

Les adapters se parlent exclusivement via des **channels `mpsc`**. Aucun adapter ne référence un autre directement.

---

## Domain

### Entités principales

| Module                  | Rôle                                                                     |
| ----------------------- | ------------------------------------------------------------------------ |
| `domain::torrent`       | `Metainfo` — contenu statique d'un fichier .torrent                      |
| `domain::tracker`       | `TrackerSession` — state machine d'announce (backoff, intervalles)       |
| `domain::tracker::http` | Encoding/parsing HTTP announce (pur)                                     |
| `domain::tracker::udp`  | Encoding/parsing UDP tracker protocol (pur)                              |
| `domain::peer`          | `PeerSession` — state machine d'une connexion peer (handshake, messages) |
| `domain::peer_pool`     | `PeerPool` — gère un ensemble dynamique de peers pour un torrent         |
| `domain::pieces`        | `PieceManager` — suivi des pièces téléchargées, sélection (rarest-first) |

### Ownership des états — règle générale

> Un état n'a **qu'un seul propriétaire**. Les autres composants en ont des **projections dérivées**, mises à jour par événements.

| État                             | Propriétaire   | Consommateurs (via événements)           |
| -------------------------------- | -------------- | ---------------------------------------- |
| Pièces vérifiées (`have`)        | `PieceManager` | `PeerPool` reçoit `PieceVerified(index)` |
| Pièces à télécharger (`needed`)  | `PeerPool`     | —                                        |
| Disponibilité peers              | `PeerPool`     | —                                        |
| Blocs demandés (exact, par peer) | `PeerSession`  | `PeerPool` (compteur dérivé seulement)   |

### `PeerSession`, `PeerPool`, `PieceManager` — trois state machines, pas de duplication

Chacun possède ce dont il a besoin pour sa responsabilité. Ils ne se contiennent pas.

|                    | `PeerSession`                                    | `PeerPool`                                                           | `PieceManager`                             |
| ------------------ | ------------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------ |
| Niveau             | protocole wire (1 peer)                          | scheduling (N peers)                                                 | assemblage et vérification                 |
| Possède            | `requested: HashSet<BlockRef>`, choke/interest   | `needed: Bitfield`, `availability`, `unchoked_by`, `in_flight_count` | `have: Bitfield`, buffer de blocs partiels |
| But de `in_flight` | protocol correctness (cancel, unexpected blocks) | décision de pipelining                                               | —                                          |
| Drivé par          | `peer_io`                                        | `peer_pool_io`                                                       | `storage_task`                             |

`PeerPool.needed` est le complément de `PieceManager.have`. Comme les state machines ne peuvent pas se requêter directement, `PeerPool` maintient `needed` en réagissant à `PieceVerified` :

```
PieceManager émet    → PieceVerified(42)
peer_pool_io traduit → PeerPool::Input::PieceVerified(42)
PeerPool met à jour  → needed.clear(42)
```

`PeerPool.in_flight_count` est un compteur dérivé mis à jour par ses propres outputs et inputs :

```
Output::RequestPiece { from, .. } → in_flight_count[from] += 1
Input::BlockReceived { from, .. } → in_flight_count[from] -= 1
Input::PeerDisconnected(addr)     → in_flight_count.remove(addr)
```

### `PeerPool`

Représente l'ensemble évolutif des peers participant au téléchargement d'**un** torrent.
Des peers peuvent apparaître (découverts par trackers, DHT, PEX) ou disparaître à tout moment.

```
Input:
  PeersDiscovered(Vec<SocketAddr>)           // tracker, DHT, PEX...
  PeerConnected { addr, peer_id, bitfield }
  PeerDisconnected(addr)
  PeerUnchokedUs(addr)
  PeerChokedUs(addr)
  PieceAvailable { addr, index }             // message Have reçu
  BlockReceived { addr, piece, offset }      // pour décrémenter in_flight_count
  PieceVerified(index)                       // hash SHA1 OK → needed.clear(index)
  Stop

Output:
  ConnectPeer(SocketAddr)
  DisconnectPeer(SocketAddr)
  RequestPiece { from: SocketAddr, index: usize }
  Completed
```

### `PieceManager`

Responsable de l'assemblage, de la vérification et de l'écriture. Ne sait rien des peers.

```
Input:
  Block { piece: usize, offset: u32, data: Bytes }

Output:
  PieceVerified(usize)    // SHA1 OK, écrit sur disque
  PieceFailed(usize)      // SHA1 KO → à re-télécharger
```

---

## Adapters

### `TrackerTransport` — abstraction interne à l'adapter

HTTP et UDP sont deux transports distincts pour contacter un tracker. Le trait `TrackerTransport` abstrait cette différence **à l'intérieur de l'adapter** — ce n'est pas un concept du domain.

```rust
// adapters/tokio_tracker_client.rs (privé)
trait TrackerTransport: Send + Sync {
    async fn announce(&self, req: &AnnounceRequest) -> Result<TrackerResponse, Error>;
}

struct HttpTransport { client: Client, url: Url }   // utilise domain::tracker::transport::http
struct UdpTransport  { host: String, port: u16 }    // utilise domain::tracker::transport::udp

pub struct TokioTrackerClient { transport: Box<dyn TrackerTransport> }
impl TrackerAnnouncer for TokioTrackerClient { ... }
```

Le domain expose le **port** (`TrackerAnnouncer`) et le **protocole pur** (`transport::http`, `transport::udp`). L'adapter assemble les deux.

### `peer_io` et `peer_pool_io` — deux adapters distincts

`peer_io` drive **une** `PeerSession` (un peer, une connexion TCP). Il traduit les outputs de `PeerSession` en événements sémantiques remontés vers `peer_pool_io`.

`peer_pool_io` drive **`PeerPool`**. Il spawne/despawne des tâches `peer_io` selon les outputs de `PeerPool`, et traduit leurs événements en inputs pour `PeerPool` :

```
peer::Output::EmitConnected       → PeerPool::Input::PeerConnected { addr, peer_id, bitfield }
peer::Output::EmitDisconnected    → PeerPool::Input::PeerDisconnected(addr)
peer::Output::EmitMessage(Have)   → PeerPool::Input::PieceAvailable { addr, index }
peer::Output::EmitMessage(Unchoke)→ PeerPool::Input::PeerUnchokedUs(addr)
```

### Acteurs (tâches tokio)

| Adapter              | Pilote           | Canal principal                                  |
| -------------------- | ---------------- | ------------------------------------------------ |
| `tracker_task`       | `TrackerSession` | → `peers_tx: Sender<Vec<SocketAddr>>`            |
| `peer_pool_io`       | `PeerPool`       | ← peers découverts, spawne N × `peer_io`         |
| `peer_io`            | `PeerSession`    | ← cmds de `peer_pool_io`, → events sémantiques   |
| `piece_manager_task` | `PieceManager`   | ← blocs reçus, → `PieceVerified` / `PieceFailed` |
| `storage_task`       | `TokioStorage`   | ← pièces vérifiées à écrire                      |

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
                    peer_pool_io
                    (drive PeerPool)
                           │ ConnectPeer    → spawn peer_io
                           │ RequestPiece   → cmd vers peer_io
                           │ DisconnectPeer → drop peer_io
                           ▼
                    peer_io × N                    ←→  réseau TCP
                    (drive PeerSession)
                           │
              ┌────────────┴────────────┐
              │ events sémantiques       │ blocs reçus (Block)
              ▼                         ▼
        peer_pool_io            piece_manager_task
        ← BlockReceived           (drive PieceManager)
        ← PieceVerified ──────────────────┐
        (needed.clear)                    │ PieceVerified
                                          ▼
                                    storage_task
                                    (écriture disque)
```

---

## Extension future

Ajouter une source de peers (DHT, PEX) = brancher un nouveau `Sender<Vec<SocketAddr>>` sur le même channel `peers_rx` du `peer_pool_task`. Le domain `PeerPool` n'a pas à changer.
