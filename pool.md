# `pool.rs` / `pieces.rs` — diagnostic et réarchitecture

## Le diagnostic en une phrase

Trois faits sont chacun stockés à deux endroits, et la cohérence entre les deux copies est maintenue à la main par le scheduler. Ce n'est pas un problème de nombre de fonctions : c'est un problème de frontières entre couches.

| Fait | Copie A | Copie B | Qui synchronise |
| --- | --- | --- | --- |
| « ce bloc est demandé » | `BlockState::Requested` (`pieces.rs:207`) | `BlockAssignments.by_peer` (`pool.rs:523`) | `send_request` écrit les deux, `reset_if_orphaned` les réconcilie |
| « le peer X a la pièce P » | `PeerState.bitfield` (`pool.rs:652`) | `PieceAvailability.by_piece` (`pool.rs:610`) | personne — les deux divergent |
| « longueur du bloc » | `BlockRange.piece_len` (`pieces.rs:53`) | `Piece::block_length` (`pieces.rs:261`) | recalculé à chaque itération |

Tant que ces doublons existent, chaque nouvelle fonctionnalité doit apprendre à maintenir les deux copies — c'est exactement ce qui s'est passé en ajoutant l'endgame.

---

## Problème 1 — L'état de requête vit dans les deux modules

`BlockState` a trois variantes, mais `Missing` et `Requested` sont **indistinguables** :

```rust
// pieces.rs:227 — le seul endroit qui lit l'état pour décider quoi demander
fn unreceived_blocks(&self) -> impl Iterator<Item = usize> + '_ {
    self.blocks.iter().enumerate().filter_map(|(index, state)| {
        (!matches!(state, BlockState::Received { .. })).then_some(index)
    })
}
```

`Missing` et `Requested` passent tous les deux le filtre. Et `reset_block` (`pieces.rs:132`) ne fait que transformer non-`Received` en `Missing`, c'est-à-dire rien d'observable. L'enum est déclaré `pub` mais n'est utilisé nulle part hors de `pieces.rs`.

Pourquoi c'est arrivé : avant l'endgame, `BlockState::Requested { at }` portait le timeout et répondait donc à « faut-il redemander ce bloc ». Depuis que `BlockAssignments.is_holder` répond à « qui doit livrer ce bloc », cette responsabilité a changé de couche mais l'ancien état est resté.

Le coût visible, c'est un invariant que le `Pool` doit se rappeler de respecter :

```rust
// pool.rs:126 — cette fonction n'existe que pour réconcilier deux états
fn reset_if_orphaned(&mut self, block_ref: BlockRef) {
    if !self.block_assignments.has_holder(block_ref) {
        self.pieces.reset_block(block_ref);
    }
}
```

Elle est appelée depuis trois chemins (`release_peer_blocks`, `RejectRequest`, le chemin bloc malformé). Oublier l'appel à un endroit, ou oublier la garde `has_holder`, casse le download d'une manière difficile à diagnostiquer.

### Solution

`PieceManager` ne doit rien savoir des requêtes. Son métier c'est « le fichier qu'on assemble » : stocker les blocs reçus, vérifier les hash, relire pour le seeding.

```rust
pub struct Piece {
    blocks: Vec<Option<Vec<u8>>>,   // reçu, ou pas
    length: usize,
    received: usize,
}
```

Disparaissent en conséquence directe :

- l'enum `BlockState`
- `PieceManager::request_block` (`pieces.rs:109`) et `Piece::request_block` (`pieces.rs:236`) — un seul appelant, `pool.rs:484`, qui jette déjà le résultat avec `let _ =`
- `PieceManager::reset_block` (`pieces.rs:132`)
- `Pool::reset_if_orphaned` et `Pool::release_peer_blocks` — libérer l'assignation devient toute l'opération
- `BlockAssignments::has_holder`

L'invariant disparaît avec l'état qu'il protégeait. C'est un retrait net, vérifiable au compilateur.

---

## Problème 2 — La vue du swarm est dupliquée, et la copie utilisée est fausse

`PeerState.bitfield` est écrit à quatre endroits, tous dans `PeerState::apply` :

| Ligne | Écriture |
| --- | --- |
| `pool.rs:684` | `self.bitfield = bf` (message `Bitfield`) |
| `pool.rs:688` | `self.bitfield.set_bit(..)` (message `Have`) |
| `pool.rs:698` | `self.bitfield.set_all()` (`HaveAll`) |
| `pool.rs:701` | `self.bitfield.unset_all()` (`HaveNone`) |

**Il n'est jamais lu.** Le scheduler interroge `PieceAvailability` (`pool.rs:431` pour `rarity`, `pool.rs:450` pour `peers_for`), pas le bitfield du peer. C'est de l'état mort qui coûte une allocation par peer et qui donne l'illusion que l'information est là.

Le vrai problème est dans l'autre copie. Les mutations de `availability` sont dispersées sur quatre sites d'appel, et **aucun ne retire** :

| Ligne | Appel | Retire ? |
| --- | --- | --- |
| `pool.rs:163` | `remove_peer` sur déconnexion | oui |
| `pool.rs:227` | `record` pour chaque pièce (`HaveAll`) | non |
| `pool.rs:272` | `record` pour chaque bit (`Bitfield`) | non |
| `pool.rs:279` | `record` (`Have`) | non |

Deux conséquences réelles :

1. **`HaveNone` ne fait rien à l'availability.** La ligne 701 vide la copie morte ; l'index, lui, garde tout. Un peer qui envoie `HaveAll` puis `HaveNone` reste enregistré comme ayant toutes les pièces, et le scheduler continuera de lui demander des blocs qu'il a annoncé ne plus avoir.
2. **Un `Bitfield` de remplacement s'accumule.** `on_message_bitfield` enregistre les bits à 1 sans retirer les précédents. L'index ne peut que croître pour un peer donné.

C'est un bug de correction, pas un smell. Et il existe précisément parce que la mise à jour de l'index est la responsabilité de l'appelant au lieu d'être encapsulée.

### Solution

Regrouper `peers` et `availability` derrière **une seule méthode de mutation**, pour que la dérive soit impossible par construction :

```rust
struct Swarm {
    peers: HashMap<SocketAddr, PeerState>,
    availability: PieceAvailability,   // index inversé, privé
}

impl Swarm {
    fn connected(&mut self, addr: SocketAddr, extensions: PeerExtensions);
    fn disconnected(&mut self, addr: SocketAddr);

    /// Le seul endroit qui touche l'availability.
    fn apply(&mut self, addr: SocketAddr, msg: &Message);

    fn peers_with(&self, piece: usize) -> impl Iterator<Item = SocketAddr> + '_;
    fn rarity(&self, piece: usize) -> usize;
    fn can_serve(&self, addr: SocketAddr, piece: usize) -> bool;
    fn unchoked(&self) -> impl Iterator<Item = SocketAddr> + '_;
}
```

`Swarm::apply` traite `Bitfield` / `Have` / `HaveAll` / `HaveNone` en un seul endroit, retraits compris : `HaveNone` et un `Bitfield` de remplacement appellent `remove_peer` avant de réenregistrer. Le bug est corrigé structurellement, pas par un appel ajouté à la main.

Et on supprime `PeerState.bitfield`, ainsi que `peer_id`, `am_choking` et `dht` — les trois que le compilateur signale déjà comme morts.

`Pool` passe de quatre champs d'état à trois : `swarm`, `assignments`, `pieces`.

---

## Problème 3 — La politique d'ordonnancement est une propriété émergente

`schedule_requests` (`pool.rs:418-479`) c'est un triple nid de boucles, un budget mutable, et quatre collaborateurs mélangés :

```rust
for depth in 0..self.peers.len() {              // profondeur de duplication
    for &piece_index in &needed {               // ordre de rareté
        for block_range in blocks {             // blocs de la pièce
            if self.block_assignments.holder_count(block_ref) != depth { continue; }
            if let Some(&addr) = self.pick_peer(..) { .. }
        }
    }
}
```

La règle réelle est simple — *servir toujours le bloc le moins couvert* — mais elle n'est écrite nulle part. Elle émerge de l'imbrication : le balayage par profondeur existe uniquement parce que la clé de tri (`holder_count`) change pendant l'allocation. Résultat, pour comprendre la politique il faut simuler les boucles dans sa tête.

Ça coûte aussi en performance : `holder_count` (`pool.rs:555`) est un scan O(nb_peers), appelé **par bloc candidat et par profondeur**.

### Solution

Sortir la politique dans une fonction dédiée, où la règle est lisible dans la clé de priorité :

```rust
struct PlannedRequest {
    block: BlockRef,
    len: usize,
    peer: SocketAddr,
}

fn plan(
    pieces: &PieceManager,
    swarm: &Swarm,
    assignments: &mut BlockAssignments,
    rng: &mut impl Rng,
) -> Vec<PlannedRequest>
```

Une seule boucle, avec un tas de priorité :

1. construire `holders: HashMap<BlockRef, usize>` en **une passe** sur `assignments` — O(N) une fois, au lieu d'un scan O(P) par bloc et par profondeur
2. empiler les blocs non reçus des pièces nécessaires, clé `(holders, rarity)` croissante
3. tant qu'il reste du budget : dépiler le bloc le moins couvert, lui choisir un peer éligible (`can_serve`, slot libre, pas déjà porteur), émettre, réempiler avec `holders + 1`. Un bloc que personne ne peut servir sort du tas définitivement.

La terminaison est évidente : chaque tour dépile, et on ne réempile qu'après avoir décrémenté un budget fini. Le balayage par profondeur disparaît — c'est la même politique, mais écrite une fois.

En conséquence, `BlockAssignments` se réduit aux opérations qui ont un sens pour ses appelants :

| Supprimé | Pourquoi |
| --- | --- |
| `holder_count` (`pool.rs:555`) | remplacé par la passe unique du scheduler |
| `has_holder` (`pool.rs:562`) | disparaît avec `reset_if_orphaned` |
| `in_flight_for` (`pool.rs:566`) | un seul appelant : `free_slots_for` |
| `has_capacity` (`pool.rs:598`) | c'est `free_slots_for(a) > 0` au point d'appel |

Il reste `assign`, `unassign`, `is_holder`, `free_slots_for`, `release_peer`, `release_expired`, `requests_in_flight`.

---

## Problème 4 — Deux endroits émettent des requêtes

`on_message_suggest_piece` (`pool.rs:389-416`) réimplémente un mini-scheduler :

```rust
for block_range in blocks {
    if !self.block_assignments.has_capacity(addr) { break; }
    if self.block_assignments.is_holder(BlockRef::from(&block_range), addr) { continue; }
    outputs.push(self.send_request(addr, block_range));
}
```

C'est la logique d'éligibilité de `pick_peer`, recopiée. Toute règle ajoutée au scheduler doit être ajoutée ici aussi, ou les deux chemins divergent.

### Solution

La suggestion devient ce qu'elle est dans BEP 6 — un indice — et non un chemin d'émission :

```rust
// PeerState
suggested: HashSet<usize>,

// on_message_suggest_piece
state.suggested.insert(piece_index);
self.interested_or_request(addr)
```

Le tas du scheduler biaise sa clé pour faire remonter les pièces suggérées. Changement de comportement assumé : la suggestion prend effet au scheduling suivant au lieu d'émettre une rafale immédiate. BEP 6 la qualifie d'advisory, donc c'est conforme.

---

## Problème 5 — Indirections résiduelles

**`BlockRange` duplique `BlockRef`.** Deux types décrivent le même bloc, avec un `From` (`pieces.rs:62`) et quatre conversions `BlockRef::from(&..)` dans `pool.rs`. La composition dit mieux ce que c'est :

```rust
pub struct BlockRange {
    pub block: BlockRef,
    pub len: usize,
}
```

Plus de champs dupliqués, plus de `From`, plus de conversions — et aucun lookup supplémentaire, contrairement à une suppression pure de `BlockRange` qui obligerait à rechercher la longueur dans `send_request`.

**`PieceEvent` a trois variantes pour deux comportements.** `BlockReceived` et `PieceInvalid` ont un traitement identique (`pool.rs:329-330`), et le `match` de `on_message_piece` est imbriqué sur deux niveaux :

```rust
pub struct CompletedPiece {
    pub piece_index: usize,
    pub piece_offset: u64,
    pub data: Vec<u8>,
}

pub fn receive_block(..) -> Result<Option<CompletedPiece>>
```

Le match s'aplatit en trois bras : `Err`, `Ok(None)`, `Ok(Some(piece))`. On perd l'information « hash invalide » ; rien ne la consomme aujourd'hui, et un scoring de peer la réintroduirait explicitement au moment d'en avoir besoin.

**La garde BEP 6 est un `match` de plus.** `on_message` (`pool.rs:168`) enchaîne deux `match message` : le premier (`pool.rs:175-186`) déconnecte les peers qui utilisent le Fast Extension sans l'avoir négocié, le second dispatche. Le premier se lit mieux en prédicat :

```rust
fn needs_fast(msg: &Message) -> bool
```

**`is_empty` induit en erreur.** `pieces.rs:128` retourne « aucune pièce n'est complète », ce qui est correct pour `HaveNone`, mais le nom se lit comme `Vec::is_empty`. `has_no_piece` dit ce que ça fait.

**`PieceManager.bitfield` est un champ public** (`pieces.rs:46`) muté en interne et lu de l'extérieur (`pool.rs:154`). Un accesseur `fn bitfield(&self) -> &Bitfield` suffit.

---

## Les couches cibles

```
PieceManager        ce qu'on assemble       blocs reçus, hash, relecture pour le seeding
Swarm               qui est là, qui a quoi  peers + index d'availability, une seule mutation
BlockAssignments    qui nous doit quoi      (peer, bloc) -> Instant
plan()              la politique            les trois ci-dessus -> Vec<PlannedRequest>
Pool                le plumbing             Input -> mutations -> Output
```

Aucune couche ne connaît celle du dessus. `PieceManager` ne sait plus ce qu'est une requête, un peer ou le temps — donc plus rien à synchroniser avec `pool.rs`.

## Ordre d'exécution proposé

Chaque étape est indépendamment livrable, et les deux premières sont des retraits nets que le compilateur valide.

| Étape | Contenu | Nature |
| --- | --- | --- |
| 1 | `BlockState` → `Option<Vec<u8>>`, suppression de `request_block` / `reset_block` / `reset_if_orphaned` | retrait net |
| 2 | `Swarm`, suppression de `PeerState.bitfield`, correctif du retrait d'availability | retrait net + correctif |
| 3 | `plan()` remplace `schedule_requests` | réécriture de la politique |
| 4 | `SuggestPiece` en indice, `BlockRange` composé, `PieceEvent` aplati, `needs_fast` | nettoyage |

L'étape 3 est la seule qui réécrit un comportement. Comme le projet n'a aucun test (`cargo test` → 0 tests), les tests de `plan()` valent d'être écrits **avant** l'étape 3, pas après.

## Bilan

Supprimés : les enums `BlockState` et `PieceEvent`, `PieceManager::request_block`, `Piece::request_block`, `PieceManager::reset_block`, `Pool::reset_if_orphaned`, `Pool::release_peer_blocks`, `BlockAssignments::{has_holder, holder_count, in_flight_for, has_capacity}`, `PeerState::{bitfield, peer_id, am_choking, dht}`, `From<&BlockRange> for BlockRef`, la boucle de `on_message_suggest_piece`, le balayage par profondeur.

Ajoutés : `Swarm` (regroupe deux champs existants), `plan()` (remplace `schedule_requests`), `CompletedPiece`, `needs_fast`.

Corrigé au passage : l'index d'availability qui ne décroissait jamais, donc le scheduler qui demandait des pièces à des peers ayant envoyé `HaveNone`.

## Tests à écrire

Dans l'ordre des étapes, en commençant par ceux qui protègent l'étape 3 :

**`Swarm`**
- `HaveNone` après `HaveAll` vide l'availability du peer
- un `Bitfield` de remplacement n'accumule pas les bits de l'ancien

**`BlockAssignments`**
- deux porteurs pour un même bloc ; `free_slots_for` reste exact après `unassign`
- `release_peer` n'affecte pas l'autre porteur
- `release_expired` ne rend que les entrées périmées

**`plan()`**
- budget abondant et beaucoup de blocs manquants → aucun duplicata
- budget supérieur au nombre de blocs non reçus → le surplus part en duplicatas, sur des peers distincts
- l'ordre de rareté est respecté
- un bloc que personne ne peut servir n'empêche pas les autres d'être planifiés

**`Pool`**
- un `Piece` venant d'un non-porteur est ignoré
- un second exemplaire d'une pièce déjà complète n'émet ni `WritePiece`, ni `Have`, ni `Completed`

**Bout en bout** sur `cosmos_laundromat.torrent` : le download se termine, et `blocks_in_flight` dépasse le nombre de blocs restants dans les derniers pourcents — c'est la preuve que l'endgame duplique.
