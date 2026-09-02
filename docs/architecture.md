# Architecture

## Principe : Sans-IO

Les composants domaine sont de pures fonctions / state machines sans IO :
- `step(input) -> Vec<Output>` : aucun effet de bord, testable sans réseau
- Les runners portent tout l'IO (TCP, UDP, channels, timers)

---

## Vue d'ensemble

```
TrackerRunner ──peers──► PoolRunner ──cmds──► PeerRunner ──TCP──► Peer
                              │
                            Pool
                     (coordonne + état peer)
```

`Pool` est le seul composant domaine stateful côté téléchargement.
`PeerSession` vit à l'intérieur de `Pool` — pas de state dupliqué.

---

## Pool

### Responsabilités

`Pool` est la state machine centrale. Elle gère :
- La découverte et connexion des peers
- L'état de chaque peer (`PeerSession` embarquée)
- Le scheduling des pièces (rarest-first, in-flight, capacité par peer)

### État interne

```rust
pub struct Pool {
    peers:        HashMap<SocketAddr, PeerSession>,  // état complet par peer
    needed:       Bitfield,                          // pièces manquantes
    in_flight:    HashMap<usize, SocketAddr>,        // pièce → peer qui la télécharge
    availability: HashMap<usize, HashSet<SocketAddr>>, // index perf: pièce → peers qui l'ont
}
```

`availability` est un index dérivé des `PeerSession.bitfield` — maintenu en sync
lors des mises à jour de bitfield. Il n'est pas une source de vérité indépendante.

### Input / Output

```rust
pub enum Input {
    PeersDiscovered(Vec<SocketAddr>),
    PeerConnected { addr: SocketAddr, peer_id: PeerId },
    PeerDisconnected(SocketAddr),
    MessageReceived { addr: SocketAddr, message: Message },
    PieceVerified(usize),
    Stop,
}

pub enum Output {
    ConnectPeer(SocketAddr),
    SendToPeer { addr: SocketAddr, message: Message },
    RequestPiece { from: SocketAddr, piece: usize }, // blocs → PieceManager (TODO)
    Completed,
}
```

`Pool` produit des `Message` réels (`Interested`, etc.) via `SendToPeer`.
Le runner ne fait que router, sans logique.

### Séquence par peer

```
PeersDiscovered      → peers.insert(addr, PeerSession::new())  → ConnectPeer
PeerConnected        → session.step(Connected)                 → (rien)
MessageReceived(Bitfield) → session.step(MessageReceived)      → schedule_requests → RequestPiece
MessageReceived(Unchoke)  → session.step(MessageReceived)      → schedule_requests → RequestPiece
MessageReceived(*)        → session.step(MessageReceived)      → SendToPeer si nécessaire
PeerDisconnected     → peers.remove(addr)                      → schedule_requests
PieceVerified        → needed.unset(piece)                     → Completed si fini
```

### schedule_requests

Lit directement l'état des `PeerSession` embarquées :
- `session.peer_choking` pour filtrer les peers unchoked
- `availability` (index) pour le rarest-first

Un seul endroit gère la logique de scheduling, avec un seul état cohérent.

---

## PeerSession (embarquée dans Pool)

`PeerSession` n'est plus un concept de runner. C'est le state per-peer que `Pool` possède.

```rust
// Pool appelle pour chaque événement peer :
let session = self.peers.get_mut(&addr)?;
for out in session.step(input) {
    match out {
        peer::Output::SendToPeer(msg) => outputs.push(Output::SendToPeer { addr, message: msg }),
        peer::Output::EmitMessage(_)  => {} // état mis à jour dans session, scheduling suit
        peer::Output::EmitConnected(_) | peer::Output::EmitDisconnected => {}
    }
}
outputs.extend(self.schedule_requests());
```

---

## PeerRunner (purement IO)

`PeerRunner` ne contient plus de `PeerSession`. Il fait uniquement :
- TCP connect + handshake (encode/decode via les fonctions domaine `Handshake`)
- Lire des `Message` depuis TCP → les envoyer au pool
- Recevoir des `Message` depuis le pool → les écrire sur TCP
- Reconnexion avec backoff (comme aujourd'hui)

```rust
pub enum PeerEvent {
    Connected(PeerId),       // handshake réussi
    Disconnected,
    MessageReceived(Message),
}
```

`PeerRunner.run` :
```rust
'run: loop {
    let (mut tcp, peer_id) = self.connect_with_retry(delay).await;
    event_tx.send(PeerEvent::Connected(peer_id)).await?;

    'session: loop {
        select! {
            msg = Message::read_from(&mut tcp) => match msg {
                Ok(msg)  => event_tx.send(PeerEvent::MessageReceived(msg)).await?,
                Err(_)   => { event_tx.send(PeerEvent::Disconnected).await?; break 'session; }
            },
            cmd = cmd_rx.recv() => match cmd {
                None      => break 'run,
                Some(msg) => { tcp.write_all(&msg.encode()).await?; }
            },
        }
    }
    delay = next_delay(delay);
}
```

Plus simple qu'aujourd'hui : pas de session, juste du IO.

---

## PoolRunner

```rust
pub struct PoolRunner {
    metainfo:   Arc<Metainfo>,
    client_id:  PeerId,
    peers_rx:   mpsc::Receiver<Vec<SocketAddr>>,          // depuis TrackerRunner
    peer_cmds:  HashMap<SocketAddr, mpsc::Sender<Message>>, // cmd vers chaque PeerRunner
    pool_rx:    mpsc::Receiver<(SocketAddr, PeerEvent)>,   // events depuis tous les PeerRunners
    pool_tx:    mpsc::Sender<(SocketAddr, PeerEvent)>,
}
```

### next_input → step → handle_output

```rust
let input = tokio::select! {
    addrs = self.peers_rx.recv() => pool::Input::PeersDiscovered(addrs),
    ev    = self.pool_rx.recv()  => match ev {
        (addr, PeerEvent::Connected(peer_id))    => pool::Input::PeerConnected { addr, peer_id },
        (addr, PeerEvent::Disconnected)          => pool::Input::PeerDisconnected(addr),
        (addr, PeerEvent::MessageReceived(msg))  => pool::Input::MessageReceived { addr, message: msg },
    },
};

for out in pool.step(input) {
    match out {
        Output::ConnectPeer(addr)              => self.spawn_peer(addr),
        Output::SendToPeer { addr, message }   => { self.peer_cmds[addr].send(message).await; },
        Output::RequestPiece { from, piece }   => { /* TODO: PieceManager → Message::Request */ },
        Output::Completed                      => return Ok(()),
    }
}
```

### spawn_peer

```rust
fn spawn_peer(&mut self, addr: SocketAddr) {
    let (cmd_tx, cmd_rx)         = mpsc::channel(128);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    self.peer_cmds.insert(addr, cmd_tx);

    let mut runner = PeerRunner::new(addr, self.client_id, self.metainfo.clone(), cmd_rx, event_tx);
    tokio::spawn(async move { runner.run().await });

    // Logging + forwarding vers pool_rx
    let pool_tx = self.pool_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            match &ev {
                PeerEvent::Connected(peer_id) => info!(addr=%addr, ?peer_id, "peer connected"),
                PeerEvent::Disconnected       => info!(addr=%addr, "peer disconnected"),
                _                             => {}
            }
            if pool_tx.send((addr, ev)).await.is_err() { break; }
        }
    });
}
```

---

## TrackerRunner (inchangé)

La logique de retry (backoff, interval) vit directement dans le runner.
Pas de state machine domaine — trop simple pour le justifier.

```rust
loop {
    sleep(interval).await;
    match client.announce(req).await {
        Ok(resp) => { interval = resp.interval; backoff = INITIAL; peers_tx.send(resp.peers)?; }
        Err(_)   => { interval = backoff; backoff = (backoff * 2).min(MAX); }
    }
}
```

---

## UDP Tracker : trait pour abstraire l'IO réseau

Même principe qu'`AsyncByteReader` : un trait injecté permet d'orchestrer
le protocole BEP15 dans le domaine sans IO réel.

```rust
// domaine
pub trait UdpSocket {
    async fn send(&self, buf: &[u8]) -> io::Result<()>;
    async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;
}

pub async fn announce<S: UdpSocket>(socket: &mut S, req: &AnnounceRequest) -> Result<TrackerResponse> {
    // connect handshake puis announce — séquencement BEP15 dans le domaine
}

// adapter
impl UdpSocket for tokio::net::UdpSocket { ... }
```

---

## Invariants

| Invariant | Garanti par |
|-----------|-------------|
| `peer_cmds` et `Pool.peers` en sync | Tous les changements passent par `pool.step` → `handle_output` |
| Pas de state dupliqué peer | `PeerSession` est la seule source de vérité per-peer |
| Pas d'IO dans le domaine | `Pool`, `PeerSession` : aucun appel système |
| `availability` dérivé de `PeerSession.bitfield` | Mis à jour à chaque `MessageReceived(Bitfield/Have)` |
