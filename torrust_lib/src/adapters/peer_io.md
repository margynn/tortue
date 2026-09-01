# PeerRunner

## Modèle mental

Séquentiel : `select!(read | cmd) → session.step → write(s)`.
Concurrence read/write ajoutée si besoin, pas avant.

```
cmd_rx ──▶┐
           ├── select! ──▶ session.step ──▶ write to TCP
 TCP read ─┘                             └▶ event_tx
```

---

## PeerRunner

```rust
struct PeerRunner<'a> {
    client_id: PeerId,
    peer_addr: SocketAddr,
    metainfo: &'a Metainfo,
}
```

---

## run() — orchestrateur

```rust
pub async fn run(&mut self, mut cmd_rx: mpsc::Receiver<Input>, event_tx: mpsc::Sender<Output>) {
    let mut session = PeerSession::new(self.peer_addr);

    loop {
        let (mut tcp, peer_id) = self.connect_with_retry(&mut session).await;

        let reconnect = self.run_session(&mut tcp, peer_id, &mut session, &mut cmd_rx, &event_tx).await;
        if !reconnect { break; }
    }
}
```

---

## connect_with_retry()

```rust
async fn connect_with_retry(&self, session: &mut PeerSession) -> (TcpStream, PeerId) {
    loop {
        match self.connect().await {
            Ok(result) => return result,
            Err(_) => {
                let retry = extract_retry(session.step(Input::ConnectionFailed));
                sleep(retry).await;
            }
        }
    }
}
```

---

## run_session() — retourne false si stop définitif, true si reconnexion

```rust
async fn run_session(
    &self,
    tcp: &mut TcpStream,
    peer_id: PeerId,
    session: &mut PeerSession,
    cmd_rx: &mut mpsc::Receiver<Input>,
    event_tx: &mpsc::Sender<Output>,
) -> bool {
    for out in session.step(Input::Connected { peer_id, num_pieces: self.metainfo.pieces.len() }) {
        if !self.handle_output(out, tcp, event_tx).await { return false; }
    }

    loop {
        let input = select! {
            res = Message::read_from(tcp) => match res {
                Ok(msg) => Input::MessageReceived(msg),
                Err(_)  => Input::Disconnected,
            },
            cmd = cmd_rx.recv() => cmd.unwrap_or(Input::Shutdown),
        };

        let disconnected = matches!(input, Input::Disconnected | Input::Shutdown);

        for out in session.step(input) {
            match self.handle_output(out, tcp, event_tx).await {
                true  => {}
                false => return !disconnected, // Stop → false, sinon reconnect
            }
        }

        if disconnected { return true; }
    }
}
```

---

## handle_output()

```rust
async fn handle_output(&self, out: Output, tcp: &mut TcpStream, event_tx: &mpsc::Sender<Output>) -> bool {
    match out {
        Output::SendToPeer(msg)  => timeout(WRITE_TIMEOUT, tcp.write_all(&msg.encode().unwrap())).await.is_ok(),
        Output::EmitConnected    => { event_tx.send(Output::EmitConnected).await.ok(); true }
        Output::EmitDisconnected => { event_tx.send(Output::EmitDisconnected).await.ok(); true }
        Output::EmitMessage(msg) => { event_tx.send(Output::EmitMessage(msg)).await.ok(); true }
        Output::ScheduleRetry(_) => false,
        Output::Stop             => false,
    }
}
```

`handle_output` retourne `false` quand il faut sortir de la session (déconnexion ou stop).

---

## Propriétés

| Propriété       | Détail                                             |
| --------------- | -------------------------------------------------- |
| Lisibilité      | `run()` tient en 5 lignes, chaque méthode = 1 rôle |
| cmd_rx          | traité via select!, pas de délai                   |
| Timeout write   | dans handle_output, borné                          |
| Reconnexion     | pilotée par la session (ScheduleRetry)             |
| Sans-IO session | PeerSession ne voit que Input/Output               |
| Concurrence     | ajoutée si nécessaire, pas avant                   |
