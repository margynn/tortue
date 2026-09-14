# Stats de session partagées Coordinator ↔ Trackers, et fin de download

## Contexte

`Output::Completed` déconnecte déjà tous les peers dans `coordinator_io.rs` (comportement voulu : on a une copie complète, pas besoin de rester connecté aux seeders). Mais `TrackerIO::run` (`tortue_lib/src/adapters/tracker_io.rs`) envoie des `SessionStats` bidons (`uploaded: 0, downloaded: 0, left: 0`, avec un `// TODO: Find a way to share session stats`), et `AnnounceEvent::Completed`/`Stopped` ne sont jamais construits (le warning de compilation `never constructed` le confirme) — seuls `Started` et `None` le sont.

Décision produit (validée) : on continue à annoncer régulièrement aux trackers après complétion (pas de `Stopped`, pas d'arrêt des tâches tracker), mais avec des stats réelles, et en signalant `event=completed` une fois. Le mécanisme de partage demandé : un objet `SessionStats` derrière un `Mutex`, partagé entre `CoordinatorIO` (writer) et chaque `TrackerIO` (reader), plutôt qu'un `watch` channel.

Un gap connexe découvert en creusant : une fois complet, `Coordinator::on_discovered` continuerait à émettre `Output::ConnectPeer` pour les peers renvoyés par les trackers (puisqu'on vient de tous les déconnecter, ils paraissent à nouveau "inconnus") — ce qui romprait l'intention de rester déconnecté. Il faut geler `ConnectPeer` une fois `pieces.is_complete()`.

Enfin, `uploaded` n'est actuellement tracké nulle part : à ajouter comme compteur cumulatif dans `Coordinator`, incrémenté exactement là où on sert un bloc (`on_message_request`).

## Changements

### 1. `tortue_lib/src/domain/coordinator/pieces.rs` — bytes téléchargés exacts, sans compteur incrémental

Ajouter deux méthodes qui **recalculent** l'état actuel plutôt que d'accumuler (évite tout risque de double-compte sur les doublons d'endgame) :

```rust
impl Piece {
    fn bytes_received(&self) -> usize {
        self.blocks.iter().enumerate()
            .filter_map(|(i, b)| matches!(b, BlockState::Received(_))
                .then(|| self.block_length(i).unwrap_or(0)))
            .sum()
    }
}

impl PieceManager {
    pub(super) fn bytes_received(&self) -> u64 {
        self.pieces.iter().map(|p| p.bytes_received() as u64).sum()
    }
}
```

### 2. `tortue_lib/src/domain/coordinator.rs` — compteur d'upload + exposition dans `CoordinatorSnapshot`

- Ajouter le champ `uploaded_bytes: u64` à `Coordinator`, initialisé à `0` dans `new`.
- Dans `on_message_request`, juste après avoir obtenu `data` de `self.pieces.read_block(...)`, faire `self.uploaded_bytes += data.len() as u64;` avant de construire l'`Output`.
- Étendre `CoordinatorSnapshot` avec `bytes_total: u64`, `bytes_done: u64`, `uploaded_bytes: u64` ; les remplir dans `snapshot()` via `self.metainfo.total_size()`, `self.pieces.bytes_received()`, `self.uploaded_bytes`. Garder `blocks_total`/`blocks_done`/`blocks_in_flight` tels quels (déjà utilisés ailleurs).
- Dans `on_discovered`, ajouter la garde : si `self.pieces.is_complete()`, ne jamais émettre `Output::ConnectPeer` (retourner `vec![]` immédiatement, ou filtrer avant la boucle) — c'est ce qui empêche de se reconnecter aux peers qu'on vient de larguer une fois les trackers réannoncés.

### 3. `tortue_lib/src/domain/tracker.rs` — `SessionStats` copiable

Ajouter `#[derive(Clone, Copy)]` sur `SessionStats` (que des `u64`, aucun souci) — nécessaire pour cloner la valeur hors du verrou avant l'appel réseau.

### 4. `tortue_lib/src/adapters/coordinator_io.rs` — writer

- Ajouter un champ `stats: Arc<Mutex<SessionStats>>` à `CoordinatorIO`, paramètre supplémentaire de `new`.
- Dans `run()`, remplacer la ligne `let _ = self.progress_tx.send(coordinator.snapshot());` par :
  ```rust
  let snapshot = coordinator.snapshot();
  *self.stats.lock().unwrap() = SessionStats {
      uploaded: snapshot.uploaded_bytes,
      downloaded: snapshot.bytes_done,
      left: snapshot.bytes_total.saturating_sub(snapshot.bytes_done),
  };
  let _ = self.progress_tx.send(snapshot);
  ```
  (le verrou std `Mutex` est relâché avant tout `.await` suivant — jamais gardé pendant une attente asynchrone.)

### 5. `tortue_lib/src/adapters/tracker_io.rs` — reader + logique `Completed`

- Ajouter un champ `stats: Arc<Mutex<SessionStats>>` à `TrackerIO`, paramètre supplémentaire de `new`.
- Dans `run()`, remplacer le `stats: SessionStats { uploaded: 0, downloaded: 0, left: 0 }` codé en dur par `let stats = *self.stats.lock().unwrap();` (clone hors verrou), utilisé dans `AnnounceRequest`.
- Ajouter un état **local** à la boucle (pas partagé) `let mut sent_completed = false;`. Décider l'événement ainsi, avant de construire la requête :
  ```rust
  let event = match next_event.take() {
      Some(e) => e,
      None if !sent_completed && stats.left == 0 => {
          sent_completed = true;
          AnnounceEvent::Completed
      },
      None => AnnounceEvent::None,
  };
  ```
  `Started` (premier tour, via `next_event`) garde la priorité ; `Completed` part exactement une fois à la première annonce où `left == 0` ; ensuite `None`.

### 6. `tortue_lib/src/application/download.rs` — câblage

- Construire une seule fois `let stats = Arc::new(Mutex::new(SessionStats { uploaded: 0, downloaded: 0, left: metainfo.total_size() }));`.
- Passer `Arc::clone(&stats)` à chaque `TrackerIO::new(url, metainfo.info_hash, node, Arc::clone(&stats))` et à `CoordinatorIO::new(..., Arc::clone(&stats))`.
- Importer `std::sync::Mutex` et `crate::domain::tracker::SessionStats`.

## Ce qui ne change pas / hors périmètre (à dire explicitement, pas à passer sous silence)

- `AnnounceEvent::Stopped` reste inutilisé : il n'existe aucun chemin d'arrêt propre de l'application aujourd'hui (pas de gestion de shutdown/ctrl-c câblée jusqu'ici) — ajouter `Stopped` sans un tel chemin n'aurait rien à déclencher. Fonctionnalité séparée si besoin plus tard.
- Fenêtre de course résiduelle très étroite : un peer déjà en cours de handshake au moment exact de `Output::Completed` peut encore atterrir en `Input::PeerConnected` juste après le passage de `is_complete()` à vrai, avant que `on_discovered` ne bloque les _futurs_ `ConnectPeer`. Non traité ici (rare, sans conséquence grave — juste une connexion qui ne servira jamais de requête).
- Pas de changement dans `peer_registry.rs` / `block_assignment.rs` / le reste du scheduler.

## Vérification

- `cargo build` : plus de warning `never constructed` sur `AnnounceEvent::Completed` (toujours un sur `Stopped`, attendu et documenté ci-dessus).
- `cargo test -p tortue_lib` : les tests existants doivent passer sans changement (aucun ne couvre `tracker_io`/`coordinator_io`, donc pas de régression attendue côté tests, mais la compilation valide le câblage).
- Relecture manuelle : simuler une petite session (2 pièces) jusqu'à `is_complete()`, vérifier que `bytes_done == bytes_total`, que `on_discovered` ne produit plus de `ConnectPeer`, et que la logique d'événement de `tracker_io` passerait de `None`/`Started` à `Completed` puis reste à `None`.

## Statut

- [ ] 1. `pieces.rs` — `bytes_received`
- [ ] 2. `coordinator.rs` — `uploaded_bytes`, `CoordinatorSnapshot` étendu, garde `on_discovered`
- [ ] 3. `tracker.rs` — `SessionStats: Clone, Copy`
- [ ] 4. `coordinator_io.rs` — writer
- [ ] 5. `tracker_io.rs` — reader + logique `Completed`
- [ ] 6. `download.rs` — câblage
