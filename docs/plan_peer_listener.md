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

Ça règle directement la remarque du cahier des charges : _"PeerIO est conçu pour la reconnexion, ça n'a pas de sens pour une connexion acceptée"_ — la reconnexion reste isolée dans `TcpPeerIO`/`TcpPeerConnector`, `PeerSession` est l'unique brique commune.

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

- Démarré **une seule fois avec le programme** (pas par torrent), reçoit `client_id: PeerId` et `SwarmRegistry` en injection (conforme à _"Listener qui démarre avec le reste du programme / Passer à swarmIO (injecter)"_).
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
- `accept_inbound` : même câblage que `spawn_peer` (créer `cmd_tx/cmd_rx`, enregistrer le sender), puis appeler `peer_connector.accept(inbound, cmd_rx, peer_events_tx)` plutôt que `connect`. Le port expose un type associé `Inbound` ; `SwarmIO` ne manipule pas le `TcpStream` et ne lance pas lui-même `run_session`. L'implémentation TCP vérifie l'info hash, conserve le mécanisme d'annulation et démarre la session sans nouveau handshake ni reconnexion.
- La session émet `PeerEvent::Connected` avec `peer_id`, `peer_extensions` et `direction`. `SwarmIO` transmet ces données à `Input::PeerConnected` ; le domaine décide quelle connexion garder selon 2.6, puis réutilise la logique `Bitfield`/`HaveAll` pour la connexion retenue. Ne pas rejeter systématiquement la seconde connexion avant cette décision : elle peut être celle qu'il faut conserver.
- Le wrapper entrant émet `Disconnected` à la fin de la session. `disconnect` doit annuler aussi bien les connexions entrantes que sortantes ; une connexion sortante rejetée comme doublon ne doit pas se reconnecter automatiquement.

### 2.6 Domaine : doublons par `PeerId` et connexions simultanées

**Politique : une seule connexion active par `(InfoHash, PeerId)`.** TCP étant full duplex, une connexion suffit pour échanger dans les deux sens. Deux connexions peuvent toutefois apparaître temporairement si les deux clients se connectent simultanément.

La déduplication par `SocketAddr` dans `on_discovered` reste utile pour éviter plusieurs tentatives vers la même adresse, mais ne remplace pas celle par `PeerId` : le port source d'une connexion entrante peut différer du port d'écoute utilisé pour la connexion sortante. IPv4 et IPv6 peuvent également conduire au même peer.

#### Données nécessaires

- Définir dans le domaine `ConnectionDirection::{Inbound, Outbound}`.
- Transporter `peer_id` et `direction` dans `PeerEvent::Connected` et `Input::PeerConnected`, en plus des extensions. La direction décrit la connexion ; elle n'appartient pas à `PeerExtensions`.
- Injecter le `PeerId` local utilisé au handshake dans `Swarm` pour comparer les identifiants. Le listener et les connectors doivent utiliser cet identifiant de façon cohérente pour le torrent.
- Stocker le `PeerId` et la direction dans `PeerRegistry`, avec un moyen de retrouver la connexion active pour un identifiant donné. La portée est le swarm, pas un registre global de peers.

#### Règle déterministe

Ne pas simplement « garder la première connexion » : lors d'une ouverture simultanée, chaque client pourrait conserver sa sortante et fermer l'entrante, ce qui fermerait les deux connexions.

Pour un doublon de directions opposées :

| Comparaison lexicographique des 20 octets | Connexion à conserver localement |
| ----------------------------------------- | -------------------------------- |
| `local_peer_id > remote_peer_id`          | `Outbound`                       |
| `local_peer_id < remote_peer_id`          | `Inbound`                        |
| Identifiants égaux                        | Refuser la connexion à soi-même  |

Les deux clients sélectionnent ainsi la même connexion physique **s'ils appliquent cette règle**. C'est notamment la stratégie présente dans libtorrent ; ce n'est pas une obligation universelle du protocole.

Pour deux connexions de même direction avec le même `PeerId`, conserver celle déjà acceptée et rejeter la nouvelle. Le `PeerId` n'est pas une identité authentifiée ; cette règle constitue une politique de gestion des connexions, pas une mesure d'authentification.

#### Application dans `Swarm::on_connected`

- Sans doublon : enregistrer la connexion et produire les messages initiaux habituels.
- Si la nouvelle connexion perd : retourner `Output::DisconnectPeer(new_addr)` sans remplacer le peer actif ni réinitialiser ses assignments.
- Si la nouvelle connexion gagne : libérer les assignments et l'état de l'ancienne, produire `Output::DisconnectPeer(old_addr)`, puis enregistrer et initialiser la nouvelle.
- Nettoyer l'association `PeerId → connexion` lors de la déconnexion **uniquement si elle désigne encore cette connexion** : le `Disconnected` tardif de l'ancienne ne doit pas retirer la nouvelle.
- Ne pas écraser un canal de commandes ou une annulation pour un `SocketAddr` encore actif. Si le code permet ultérieurement de réutiliser une adresse avant la fin de l'ancienne tâche, identifier les événements par un identifiant de connexion pour éviter le nettoyage de la mauvaise session.

La décision reste dans le domaine, pure et testable ; les adapters exécutent les déconnexions et arrêtent toute reconnexion pour le runner rejeté.

#### Tests minimaux

- Un doublon de même direction est rejeté sans modifier le peer actif.
- Pour chacune des deux comparaisons de `PeerId`, tester les deux ordres d'arrivée (`Inbound` puis `Outbound`, et inversement) : la direction conservée doit être identique.
- Simuler les deux côtés d'une ouverture simultanée : ils doivent conserver la même connexion physique.
- Après remplacement, un `Disconnected` tardif de l'ancienne connexion ne retire ni le nouveau peer ni ses assignments.
- Une connexion sortante rejetée ne relance pas une boucle de reconnexion.

Références : [BEP 11, déduplication IPv4/IPv6](https://www.bittorrent.org/beps/bep_0011.html) et [gestion des doublons dans libtorrent](https://github.com/arvidn/libtorrent/blob/RC_2_0/src/bt_peer_connection.cpp). Libtorrent dispose également d'une option autorisant plusieurs connexions par `PeerId` ; Tortue conserve ici la politique simple d'une seule connexion.

## 3. Ordre d'implémentation suggéré

1. Déplacer `Handshake` dans le domaine (pas de changement de comportement, juste un déplacement + adaptation des imports). Commit isolé, facile à vérifier par les tests existants.
2. Extraire `PeerSession` de `TcpPeerIO` sans rien changer côté `TcpPeerConnector` (refactor pur, tests de non-régression sur le sortant).
3. Ajouter `peer_id` et `ConnectionDirection` aux événements de connexion, injecter le `PeerId` local dans `Swarm`, puis implémenter la résolution déterministe des doublons dans `PeerRegistry`/`Swarm::on_connected` avec les tests de 2.6 (deux ordres d'arrivée, remplacement et nettoyage tardif).
4. Introduire `SwarmRegistry` (application) + branchement dans `start_download` (register/unregister).
5. Implémenter `PeerListenner::handle_peer` (lecture handshake, route, réponse handshake, envoi `InboundPeer`).
6. Brancher `inbound_rx` dans `SwarmIO::run` + `accept_inbound`.
7. Démarrer `PeerListenner` une fois dans `main.rs` (ou équivalent), injecté avec le `client_id` et le `SwarmRegistry` partagé par tous les `Download`.

## 4. Points ouverts / à trancher avec toi

- **Où vit la session/le registre global ?** Aujourd'hui rien ne regroupe les `Download` entre eux (`download()`/`download_magnet()` sont des fonctions libres). Il faut probablement introduire un petit type `Session`/`Client` possédant `SwarmRegistry` + le `JoinHandle` du `PeerListenner`, construit une fois dans `main.rs`, et passé/cloné à chaque appel de `start_download`. À confirmer que c'est acceptable comme changement d'API publique de `tortue_lib`.
- **Port d'écoute** : actuellement en dur (`8080`) dans `PeerListenner::main_loop`. À paramétrer (config/CLI) ou garder en dur pour une première version ?
- **Handshake entrant sans timeout actuellement précisé** : je propose de réutiliser `TcpPeerIO::CONNECT_TIMEOUT`/`READ_TIMEOUT` (le renommer en constante partagée, ex. `Handshake::TIMEOUT`) pour éviter qu'un pair malveillant garde la socket ouverte indéfiniment sans envoyer son handshake.
