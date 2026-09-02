# Architecture

## Principe : Sans-IO

Les composants domaine (`PeerSession`, `Pool`) sont de pures state machines sans IO :

- `step(input) -> Vec<Output>` : aucun effet de bord, testable sans réseau
- Les runners (`PeerRunner`, `PoolRunner`, `TrackerRunner`) portent tout l'IO

---

## Couches

```
TrackerRunner ──peers──► PoolRunner ──cmds──► PeerRunner ──TCP──► Peer
                 │            │                    │
             TrackerClient  Pool              PeerSession
             (IO)           (domaine)         (domaine)
```

---

## Pool dépend de peer::Output

`Pool` interprète les événements des peers directement. Pas de translation dans le runner.

### Pool::Input

```rust
pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    FromPeer { addr: SocketAddr, event: peer::Output },
    PieceVerified(usize),
    Stop,
}
```

`peer::Output` porte déjà le vocabulaire des événements peer (`EmitConnected`,
`EmitDisconnected`, `EmitMessage`). Le runner passe ces événements tels quels.
`SendToPeer` n'arrive jamais ici — le `PeerRunner` le consume lui-même.

### Pool::Output

```rust
pub enum Output {
    SpawnPeer(SocketAddr),
    DropPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, cmd: peer::Input },
    RequestPiece { from: SocketAddr, piece: usize },  // blocs gérés par PieceManager
    Completed,
}
```

`Pool` produit des commandes en termes domaine. C'est le runner qui traduit
`RequestPiece` en `Message::Request { begin, length }` via le `PieceManager`.

---

## PoolRunner : next_input → step → handle_output

```rust
async fn next_input(&mut self) -> pool::Input {
    tokio::select! {
        addrs = self.peers_rx.recv()  => pool::Input::PeersDiscovered(addrs),
        msg   = self.pool_rx.recv()   => pool::Input::FromPeer { addr: msg.0, event: msg.1 },
    }
}

fn handle_output(&mut self, out: pool::Output) {
    match out {
        Output::SpawnPeer(addr)           => self.spawn_peer(addr),
        Output::DropPeer(addr)            => { self.peer_cmds.remove(&addr); },
        Output::SendToPeer { addr, cmd }  => { self.peer_cmds[addr].send(cmd); },
        Output::RequestPiece { from, piece } => {
            // TODO: PieceManager → blocs manquants → Message::Request
        },
        Output::Completed => { /* signal */ },
    }
}
```

`pool_rx: mpsc::Receiver<(SocketAddr, peer::Output)>` est alimenté par un forwarding
task par peer (cf. `spawn_peer`).

---

## Pool::step : séquencement des événements peer

La séquence BitTorrent après connexion :

1. `EmitConnected(peer_id)` → peer connu, aucune pièce encore
2. `EmitMessage(Bitfield)` _(optionnel)_ → disponibilité initiale
3. `EmitMessage(Have)` → mises à jour ultérieures

`Pool::step(FromPeer { EmitConnected })` → transite le peer en `Connected` (bitfield vide).
`Pool::step(FromPeer { EmitMessage(Bitfield) })` → met à jour la disponibilité.

Pas besoin de bufferiser dans le runner — le pool gère la séquence en interne.

---

## UDP Tracker : trait pour abstraire l'IO réseau

Même principe qu'`AsyncByteReader` dans `peer/message.rs` : un trait injecté permet
d'orchestrer le protocole dans le domaine sans IO réel.

### Trait (domaine)

```rust
pub trait UdpSocket {
    async fn send(&self, buf: &[u8]) -> io::Result<()>;
    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;
}
```

### Orchestration (domaine)

Le séquencement BEP15 (connect → announce) vit dans le domaine :

```rust
pub async fn announce<S: UdpSocket>(socket: &mut S, req: &AnnounceRequest) -> Result<TrackerResponse> {
    // connect handshake
    let tx_id = rand::random::<u32>();
    socket.send(&build_connect_request(tx_id)).await?;
    let n = socket.recv(&mut buf).await?;
    let connection_id = parse_connect_response(&buf[..n], tx_id)?;

    // announce
    socket.send(&build_announce_request(connection_id, rand::random(), rand::random(), req)).await?;
    let n = socket.recv(&mut buf).await?;
    parse_announce_response(&buf[..n])
}
```

### Adapter

```rust
impl UdpSocket for tokio::net::UdpSocket {
    async fn send(&self, buf: &[u8]) -> io::Result<()> { self.send(buf).await.map(|_| ()) }
    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.recv(buf).await }
}
```

`UdpTransport::announce` dans l'adapter se réduit à : DNS → bind → connect → appel domaine.

---

## TrackerRunner : pas de state machine

La logique de retry (backoff, interval) est triviale et vit directement dans le runner.
`TrackerSession` a été supprimé.

```rust
loop {
    sleep(interval).await;
    match client.announce(req).await {
        Ok(resp) => { interval = resp.interval; backoff = INITIAL; peers_tx.send(resp.peers); }
        Err(_)   => { interval = backoff; backoff = (backoff * 2).min(MAX); }
    }
}
```
