# Plan : connexions entrantes (`PeerListenner`)

## 1. Constat sur l'existant

- `TcpPeerConnector`/`TcpPeerIO` (`adapters/peer_io.rs`) ne gèrent que les connexions **sortantes** : handshake + boucle de reconnexion avec backoff. Le handshake (`Handshake::encode/decode`) est une struct privée de l'adapter, pure (pas d'IO), mais pas réutilisable ailleurs.
- `SwarmIO` (`adapters/swarm_io.rs`) est instancié **une fois par torrent** (`download.rs::start_download`), avec son propre `TcpPeerConnector`. Il n'existe aucun registre global des torrents/`Metainfo` connus — chaque `Download` vit isolé.
- `Swarm` (`domain/swarm.rs`) + `PeerRegistry` (`domain/swarm/peer_registry.rs`) ne connaissent les pairs **que par `SocketAddr`** : le `PeerId` reçu au handshake est jeté après le log dans `SwarmIO::run` (`info!(peer_id = %peer_id, ...)`). Impossible aujourd'hui de détecter un `peer_id` déjà connecté.
- `PeerListenner` (TODO en bas de `peer_io.rs`) bind déjà les sockets IPv4/IPv6 et boucle sur `accept()`, mais `handle_peer` est un stub.

Conséquence : pour accepter des connexions entrantes il manque (a) un point d'entrée unique démarré avec le programme, (b) un registre global reliant `InfoHash → swarm`, (c) un moyen de router une connexion acceptée vers le bon `SwarmIO`, et (d) la détection de doublon par `PeerId` dans le domaine.

## 2. Découpage proposé

### 2.1 Extraire le handshake vers le domaine

Déplacer la struct `Handshake` (et `Message::frame`/`read_from`, déjà dans `domain::message` normalement) de `adapters/peer_io.rs` vers `domain/peer.rs` (ou un nouveau `domain/handshake.rs`). Elle est déjà pure : `encode()`/`decode()` ne touchent à aucune IO, seulement `InfoHash`/`PeerId`. Ça permet de la réutiliser telle quelle depuis `TcpPeerConnector` (sortant) et depuis `PeerListenner` (entrant) sans dépendre d'un détail d'implémentation d'un adapter.

### 2.2 Séparer "connexion" et "session" dans `peer_io.rs`

`TcpPeerIO::run` mélange aujourd'hui deux responsabilités :
1. **Établir** la connexion (connect + handshake + retry/backoff) — n'a de sens que pour le sortant.
2. **Faire vivre** une connexion déjà établie (lecture/écriture de `Message`, keepalive, `spawn_reader`, sélection sur `cmd_rx`/`cancel_rx`) — identique que la connexion soit entrante ou sortante.

Proposition : extraire (2) dans une fonction/struct neutre, par ex. `PeerSession::run(stream, peer_addr, cmd_rx, evt_tx, cancel_rx)`, qui ne connaît ni le handshake ni la reconnexion. `TcpPeerIO` (sortant) devient : boucle de retry + handshake, puis délégation à `PeerSession::run`. Le flux entrant fera : handshake entrant (une seule tentative, pas de retry), puis la même délégation à `PeerSession::run`.

Ça règle directement la remarque du cahier des charges : *"PeerIO est conçu pour la reconnexion, ça n'a pas de sens pour une connexion acceptée"* — la reconnexion reste isolée dans `TcpPeerIO`/`TcpPeerConnector`, `PeerSession` est l'unique brique commune.

### 2.3 Registre des metainfo/swarms connus

Nouveau type applicatif, par ex. `application::registry::SwarmRegistry` (ou dans un nouveau fichier `application/swarm_registry.rs`) :

```rust
#[derive(Clone, Default)]
pub struct SwarmRegistry {
    inner: Arc<Mutex<HashMap<InfoHash, mpsc::Sender<InboundPeer>>>>,
}
```

- `register(info_hash, tx)` / `unregister(info_hash)` : appelés par `start_download` quand un `SwarmIO` démarre/s'arrête.
- `route(info_hash) -> Option<Sender<InboundPeer>>` : utilisé par `PeerListenner` pour savoir si le metainfo est connu et à qui transmettre la connexion.
- `InboundPeer { addr, peer_id, extensions, stream }` : ce que `PeerListenner` transmet une fois le handshake entrant décodé et répondu.

Ce registre est **global au process** (un seul, partagé entre tous les `Download`), à l'inverse de `SwarmIO` qui reste par torrent. Il faut donc le faire remonter un niveau au-dessus de `download.rs` (ex: un `Session`/`Client` construit une fois au démarrage du programme, possédant le `SwarmRegistry` + le `PeerListenner`, et chaque appel à `download()`/`download_magnet()` passe par cette session pour s'enregistrer). Concrètement ça touche `main.rs` et `application/download.rs::start_download` (nouveau paramètre `registry: SwarmRegistry`).

### 2.4 `PeerListenner` : finir l'implémentation

- Démarré **une seule fois avec le programme** (pas par torrent), reçoit `client_id: PeerId` et `SwarmRegistry` en injection (conforme à *"Listener qui démarre avec le reste du programme / Passer à swarmIO (injecter)"*).
- `handle_peer(stream)` :
  1. Lire le handshake entrant avec timeout (`Handshake::decode`, réutilisant le code déplacé en 2.1).
  2. `registry.route(info_hash)` :
     - `None` → **abandon** (drop du stream), conforme au cahier des charges.
     - `Some(tx)` → répondre au handshake (+ extension handshake BEP10 si négocié, même logique que `TcpPeerIO::connect`), puis envoyer `InboundPeer{..}` sur `tx`. Le `SwarmIO` cible décide ensuite seul (pas de race : un seul writer sur son état) si le `peer_id` est un doublon.
  3. En cas d'erreur à n'importe quelle étape → log + drop.

### 2.5 `SwarmIO` : accepter les connexions entrantes

- Nouveau champ `inbound_rx: mpsc::Receiver<InboundPeer>` construit avec `SwarmIO::new`, et `registry.register(info_hash, inbound_tx)` fait soit dans `SwarmIO::new`, soit explicitement dans `start_download` juste avant de spawn `coordinator.run()`. Il faut aussi `registry.unregister(info_hash)` quand `run()` se termine (y compris sur erreur — `defer`/`scopeguard` ou simplement dans le bloc `Err`/fin de fonction).
- Nouvelle branche dans le `tokio::select!` de `SwarmIO::run` :
  ```rust
  inbound = self.inbound_rx.recv() => match inbound {
      Some(peer) => self.accept_inbound(peer, &mut coordinator),
      None => continue, // registry fermé, pas fatal
  }
  ```
- `accept_inbound` : vérifie le doublon de `peer_id` (nouvelle info portée par `Input`, voir 2.6) ; si doublon → drop le stream (ne rien spawn) ; sinon, même câblage que `spawn_peer` aujourd'hui (créer `cmd_tx/cmd_rx`, l'insérer dans `peer_cmds`), mais au lieu d'appeler `peer_connector.connect(addr, ...)` (qui referait un handshake sortant), on spawn directement `PeerSession::run(stream, addr, cmd_rx, peer_events_tx)` (cf. 2.2) puisque le handshake a déjà eu lieu côté `PeerListenner`.
- Émet ensuite `Input::PeerConnected { addr, peer_id, peer_extensions }` vers `coordinator.step` (même chemin que pour le sortant, donc même logique `Bitfield`/`HaveAll` etc. réutilisée gratuitement).

### 2.6 Domaine : détection de doublon par `PeerId`

- `PeerExtensions`/`Input::PeerConnected` doit transporter le `peer_id` (actuellement jeté). Ajouter le champ dans `Input::PeerConnected`.
- `PeerRegistry` (domain) doit garder une table `peer_id → addr` (ou stocker `peer_id` dans `PeerState`) pour pouvoir répondre à `contains_peer_id(id) -> bool`.
- `Swarm::on_connected` (ou une nouvelle étape avant, côté `SwarmIO::accept_inbound`) refuse l'ajout si le `peer_id` est déjà présent. Deux options :
  - (a) vérif faite côté `SwarmIO` avant même d'appeler `coordinator.step` (évite de complexifier `Input`/`Output` avec un cas de rejet) — plus simple, mais duplique un peu la logique de connu/pas connu.
  - (b) vérif entièrement dans `Swarm::on_connected`, qui retournerait `Output::DisconnectPeer` au lieu d'ajouter le pair si le `peer_id` est dupliqué — plus propre car toute la logique métier reste dans le domaine pur, testable sans IO.
  - **Recommandation : (b)**, cohérent avec le reste de `Swarm` qui concentre toutes les règles métier et reste 100% testable sans tokio.

## 3. Ordre d'implémentation suggéré

1. Déplacer `Handshake` dans le domaine (pas de changement de comportement, juste un déplacement + adaptation des imports). Commit isolé, facile à vérifier par les tests existants.
2. Extraire `PeerSession` de `TcpPeerIO` sans rien changer côté `TcpPeerConnector` (refactor pur, tests de non-régression sur le sortant).
3. Ajouter `peer_id` à `Input::PeerConnected` + table de doublons dans `PeerRegistry` (domaine), avec un test unitaire dédié ("deuxième connexion avec le même `peer_id` → `DisconnectPeer`").
4. Introduire `SwarmRegistry` (application) + branchement dans `start_download` (register/unregister).
5. Implémenter `PeerListenner::handle_peer` (lecture handshake, route, réponse handshake, envoi `InboundPeer`).
6. Brancher `inbound_rx` dans `SwarmIO::run` + `accept_inbound`.
7. Démarrer `PeerListenner` une fois dans `main.rs` (ou équivalent), injecté avec le `client_id` et le `SwarmRegistry` partagé par tous les `Download`.

## 4. Points ouverts / à trancher avec toi

- **Où vit la session/le registre global ?** Aujourd'hui rien ne regroupe les `Download` entre eux (`download()`/`download_magnet()` sont des fonctions libres). Il faut probablement introduire un petit type `Session`/`Client` possédant `SwarmRegistry` + le `JoinHandle` du `PeerListenner`, construit une fois dans `main.rs`, et passé/cloné à chaque appel de `start_download`. À confirmer que c'est acceptable comme changement d'API publique de `tortue_lib`.
- **Port d'écoute** : actuellement en dur (`8080`) dans `PeerListenner::main_loop`. À paramétrer (config/CLI) ou garder en dur pour une première version ?
- **Handshake entrant sans timeout actuellement précisé** : je propose de réutiliser `TcpPeerIO::CONNECT_TIMEOUT`/`READ_TIMEOUT` (le renommer en constante partagée, ex. `Handshake::TIMEOUT`) pour éviter qu'un pair malveillant garde la socket ouverte indéfiniment sans envoyer son handshake.
