# Pièces sur disque, cache d’upload et Cancel

## Contrat

- Une pièce **partielle** garde ses blocs en RAM. Une pièce **complète et vérifiée** ne garde que son état et sa longueur ; ses données partent dans `WritePiece`.
- Pour un upload : cache → réponse immédiate ; cache miss → attente et **une lecture partagée par pièce**.
- `Cancel` retire uniquement la demande `(peer, piece_index, offset, len)`. Il annule la lecture seulement s’il ne reste aucun demandeur.
- Un résultat annulé/tardif ne répond à personne et ne remplit pas le cache. Un Cancel après émission de `SendToPeer` ne fait rien ; il ne purge pas une pièce déjà cachée.
- Annuler signifie abandonner le résultat et éviter une lecture encore en queue. Une IO déjà commencée peut finir : **ne jamais abort le worker disque**.

On conserve `PieceStore`, `DiskCommand::Read`, la queue FIFO et le flush de completion. Pas de seconde abstraction de stockage.

## 1. `piece_manager.rs` : séparer état et données

Conserver `Piece.length` pour calculer les tailles et les compteurs sans blocs en mémoire :

```rust
struct Piece {
    length: usize,
    state: PieceState,
}

enum PieceState {
    Partial {
        blocks: Vec<BlockState>,
        received: usize,
    },
    Complete,
}
```

Adapter les méthodes existantes :

| Méthode               | Partial                                 | Complete                                       |
| --------------------- | --------------------------------------- | ---------------------------------------------- |
| `is_complete()`       | `false`                                 | `true`                                         |
| nombre de blocs       | `length.div_ceil(BLOCK_SIZE)`           | idem                                           |
| blocs reçus           | `received`                              | nombre de blocs                                |
| `unreceived_blocks()` | blocs Missing                           | aucun                                          |
| `is_partial()`        | `received > 0`                          | `false`                                        |
| `receive_block()`     | comportement actuel                     | `Ok(false)` après validation de l’index/taille |
| `buffer()`            | concaténer si tous les blocs sont reçus | inutile                                        |
| `reset()`             | réinitialiser les blocs                 | recréer Partial si nécessaire                  |

**Attention :** aujourd’hui `is_complete()` signifie aussi « tous les blocs reçus, avant SHA-1 ». Avec l’enum, utiliser un `all_blocks_received()` distinct dans `receive_block()` :

```rust
// Dans PieceManager::receive_block, après ingestion du bloc :
if !p.receive_block(block_index, data)? || !p.all_blocks_received() {
    return Ok(None);
}
let buffer = p.buffer().expect("all blocks received");
if !verify_piece_hash(self.metainfo.pieces[piece_index], &buffer) {
    p.reset();
    self.bitfield.unset_bit(piece_index)?;
    return Ok(None);
}
p.state = PieceState::Complete; // libère les Vec des blocs
self.bitfield.set_bit(piece_index)?;
// Retourner CompletedPiece comme aujourd’hui : buffer est déplacé, pas cloné.
```

`blocks_total()`, `blocks_received()`, `available_bytes()` et le scheduler doivent utiliser ces méthodes, pas les anciens champs `blocks`/`received`.

Restoration : garder `Input::LocalPiece` et `on_local_piece()`. Une pièce locale valide devient Complete sans WritePiece, sans trafic réseau et sans garder ses données. Une pièce invalide reste à télécharger.

## 2. Cache pondéré : 50 MiB par défaut

Ajouter `quick_cache = "0.7"` dans `tortue_lib/Cargo.toml`. Utiliser `unsync` : le domaine est manipulé par une seule boucle.

```rust
use quick_cache::{unsync::Cache, Weighter};

const DEFAULT_UPLOAD_CACHE_BYTES: usize = 50 * 1024 * 1024;

struct PieceWeighter;
impl Weighter<usize, Vec<u8>> for PieceWeighter {
    fn weight(&self, _: &usize, data: &Vec<u8>) -> u64 {
        data.capacity() as u64
    }
}

// Champs supplémentaires de PieceManager :
// cache: Cache<usize, Vec<u8>, PieceWeighter>,
// cache_bytes: usize,

// Dans new() :
let cache = Cache::with_weighter(
    (DEFAULT_UPLOAD_CACHE_BYTES / metainfo.piece_length.max(1)).max(1),
    DEFAULT_UPLOAD_CACHE_BYTES as u64,
    PieceWeighter,
);
```

Deux méthodes :

```rust
fn set_cache_bytes(&mut self, bytes: usize) {
    self.cache_bytes = bytes;
    self.cache.set_capacity(bytes as u64);
}

fn cache_piece(&mut self, index: usize, data: Vec<u8>) {
    // Le cache peut conserver une entrée surdimensionnée selon sa politique.
    // Refuser explicitement les pièces plus grandes que le budget ; 0 désactive.
    if self.cache_bytes > 0 && data.capacity() <= self.cache_bytes {
        self.cache.insert(index, data);
    }
}
```

Rendre ces méthodes `pub(super)` et initialiser `cache_bytes` à `DEFAULT_UPLOAD_CACHE_BYTES`. Configuration minimale : ajouter `SwarmCommand::SetUploadCacheBytes(usize)`, puis déléguer à `self.pieces.set_cache_bytes(bytes)` dans `on_swarm_command()`. Exposer dans `SwarmHandle` :

```rust
pub async fn set_upload_cache_bytes(&self, bytes: usize) -> Result<()> {
    self.send(SwarmCommand::SetUploadCacheBytes(bytes)).await
}
```

Pas de changement des constructeurs ni d’option CLI dans cette étape.

Séparer **requête valide** et **cache hit** : `read_block() == None` ne doit pas signifier systématiquement « charger depuis disque ».

```rust
fn valid_upload_range(&self, index: usize, offset: usize, len: usize) -> bool {
    self.pieces.get(index).is_some_and(|p| {
        p.is_complete()
            && len > 0
            && len <= BLOCK_SIZE
            && offset.checked_add(len).is_some_and(|end| end <= p.length)
    })
}

fn read_block(&mut self, index: usize, offset: usize, len: usize) -> Option<Vec<u8>> {
    if !self.valid_upload_range(index, offset, len) {
        return None;
    }
    Some(self.cache.get(&index)?.get(offset..offset.checked_add(len)?)?.to_vec())
}
```

Ne pas imposer l’alignement de `BlockRef` aux uploads : une requête peut viser un sous-intervalle valide.

**Choix simple :** remplir le cache seulement sur lecture disque, pas lors de `CompletedPiece`. Cela évite de cloner la pièce pour la garder ET l’écrire. L’éviction ne change jamais le bitfield.

Le budget concerne les buffers du cache, **pas toute la RAM du client** : pièces partielles, queue d’écriture et blocs en attente réseau sont hors budget.

## 3. `swarm.rs` : types et état d’attente

Ne pas réutiliser `BlockAssignments` : il suit nos téléchargements, pas les requêtes d’upload reçues.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PieceReadId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UploadRequest {
    addr: SocketAddr,
    offset: usize,
    len: usize,
}

struct PendingPieceRead {
    id: PieceReadId,
    requests: HashSet<UploadRequest>,
}

// Swarm :
// pending_uploads: HashMap<usize, PendingPieceRead>,
// next_piece_read_id: u64, // initialisé à 0

// Output :
// ReadPiece { id: PieceReadId, piece_index: usize, offset: u64, len: usize },
// CancelPieceRead(PieceReadId),

// Input :
// PieceLoaded { id: PieceReadId, piece_index: usize, data: Vec<u8> },
```

L’identifiant est **par lecture**, jamais seulement par pièce. Sinon une ancienne lecture de A, annulée puis relancée, pourrait satisfaire les demandes de la nouvelle lecture.

### Request

Dans `on_message_request()` :

1. Refuser si upload désactivé ou intervalle invalide/non vérifié.
2. Si `read_block()` trouve les données : produire le `SendToPeer` existant et incrémenter `uploaded_bytes` comme aujourd’hui.
3. Sinon construire `UploadRequest`. Si déjà pending, ne rien ajouter.
4. Si cette pièce a déjà une lecture : ajouter le demandeur, sans nouvelle IO.
5. Sinon allouer un ID, enregistrer l’attente et produire `ReadPiece`.

Voici le chemin cache miss, après les gardes et le cache hit existants :

```rust
const MAX_PIECE_READS: usize = 8;
const MAX_PENDING_PER_PEER: usize = 32;
const MAX_PENDING_UPLOADS: usize = 256;

// Dans on_message_request() :
let request = UploadRequest { addr, offset: piece_offset, len: piece_len };
if self.pending_uploads.get(&piece_index)
    .is_some_and(|p| p.requests.contains(&request))
{
    return vec![];
}
let total: usize = self.pending_uploads.values().map(|p| p.requests.len()).sum();
let per_peer = self.pending_uploads.values()
    .flat_map(|p| &p.requests).filter(|r| r.addr == addr).count();
let new_read = !self.pending_uploads.contains_key(&piece_index);
if total >= MAX_PENDING_UPLOADS
    || per_peer >= MAX_PENDING_PER_PEER
    || (new_read && self.pending_uploads.len() >= MAX_PIECE_READS)
{
    let mut out = self.on_disconnected(addr); // inclut le nettoyage des uploads
    out.push(Output::DisconnectPeer(addr));
    return out;
}
if let Some(pending) = self.pending_uploads.get_mut(&piece_index) {
    pending.requests.insert(request);
    return vec![];
}
let id = PieceReadId(self.next_piece_read_id);
self.next_piece_read_id = self.next_piece_read_id.checked_add(1)
    .expect("piece read id exhausted");
self.pending_uploads.insert(piece_index, PendingPieceRead {
    id,
    requests: HashSet::from([request]),
});
vec![Output::ReadPiece {
    id,
    piece_index,
    offset: piece_index as u64 * self.metainfo.piece_length as u64,
    len: self.pieces.piece_length(piece_index).expect("validated piece index"),
}]
```

Les constantes sont au niveau du module. Ajouter `HashMap` aux imports de `swarm.rs` et rendre `valid_upload_range()` / `read_block()` accessibles avec `pub(super)`.

Ajouter `PieceManager::piece_length(index) -> Option<usize>` retournant `Piece.length`, notamment pour la dernière pièce.

**Bornes dès cette étape :** 8 lectures logiques simultanées, 32 demandes pending par peer, 256 au total. Vérifier les doublons avant les limites. En dépassement, nettoyer les attentes du peer puis produire `DisconnectPeer` ; ne pas allouer une attente illimitée ni ignorer silencieusement une nouvelle demande. Ces limites ne s’appliquent pas aux cache hits.

### Cancel exact

Router `Message::Cancel { piece_index, piece_offset, piece_len }` vers :

```rust
fn on_message_cancel(&mut self, addr: SocketAddr, index: usize, offset: usize, len: usize)
    -> Vec<Output>
{
    let Some(pending) = self.pending_uploads.get_mut(&index) else {
        return vec![];
    };
    pending.requests.remove(&UploadRequest { addr, offset, len });
    if !pending.requests.is_empty() {
        return vec![];
    }
    let id = pending.id;
    self.pending_uploads.remove(&index);
    vec![Output::CancelPieceRead(id)]
}
```

Un Cancel d’un peer ne supprime jamais les demandes d’un autre peer.

### PieceLoaded

```rust
fn on_piece_loaded(&mut self, id: PieceReadId, index: usize, data: Vec<u8>) -> Vec<Output> {
    if !self.pending_uploads.get(&index).is_some_and(|p| p.id == id) {
        return vec![]; // annulé ou ancienne génération : pas de cache
    }
    let pending = self.pending_uploads.remove(&index).expect("checked above");
    let mut out = vec![];
    if self.status.upload() {
        for request in pending.requests {
            if !self.peer_registry.contains_addr(request.addr) {
                continue;
            }
            let Some(block) = data.get(request.offset..request.offset + request.len) else {
                continue; // intervalle validé à l'entrée ; garde défensive
            };
            self.uploaded_bytes += block.len() as u64;
            out.push(Output::SendToPeer {
                addr: request.addr,
                message: Message::Piece {
                    piece_index: index,
                    piece_offset: request.offset,
                    data: block.to_vec(),
                },
            });
        }
    }
    self.pieces.cache_piece(index, data);
    out
}
```

**Servir à partir de `data` AVANT son insertion** : une pièce trop grande pour le cache doit quand même satisfaire les demandes, sans boucle de relecture.

### Nettoyage lifecycle

Une seule passe avec `HashMap::retain`, sans liste d’indices intermédiaire :

```rust
fn cancel_peer_uploads(&mut self, addr: SocketAddr) -> Vec<Output> {
    let mut out = vec![];
    self.pending_uploads.retain(|_, pending| {
        pending.requests.retain(|request| request.addr != addr);
        if pending.requests.is_empty() {
            out.push(Output::CancelPieceRead(pending.id));
            false
        } else {
            true
        }
    });
    out
}

fn cancel_all_uploads(&mut self) -> Vec<Output> {
    self.pending_uploads.drain()
        .map(|(_, pending)| Output::CancelPieceRead(pending.id))
        .collect()
}
```

Dans `SetStatus`, enregistrer le nouveau statut puis retourner `cancel_all_uploads()` si `!self.status.upload()`, sinon `vec![]`.

- `on_disconnected()` doit retourner ces outputs, pas toujours `vec![]`.
- Dans `on_connected()`, remplacer l’appel actuellement ignoré à `on_disconnected(old_addr)` par `out.extend(self.on_disconnected(old_addr))`.
- Nettoyer également lors d’un `DisconnectPeer` décidé localement, avant émission : violation de protocole, dépassement des limites, remplacement de connexion, etc. Ne pas attendre un événement réseau pour éviter qu’une nouvelle connexion au même `SocketAddr` récupère une vieille réponse.
- Si `SetStatus` désactive l’upload : vider toutes les attentes et émettre tous les cancels. `UploadOnly` conserve les uploads ; `DownloadOnly` et `Stopped` les annulent.
- `step()` doit router `PieceLoaded` vers `on_piece_loaded()`.

## 4. `PieceStore` : lecture détachable sans emprunter le stockage

**Ne pas faire `piece_store.read(...).await` dans `handle_output()`** : la boucle serait bloquée et ne pourrait pas recevoir Cancel. Mais l’actuelle `async fn read(&mut self, ...)` garde aussi un emprunt incompatible avec une tâche détachée.

Changer uniquement la signature de `read` ; conserver `write` et `flush` :

```rust
use std::future::Future;

pub trait PieceStore: Send {
    async fn write(&mut self, offset: u64, data: Vec<u8>) -> std::io::Result<()>;
    async fn flush(&mut self) -> std::io::Result<()>;
    fn read(&mut self, offset: u64, len: usize)
        -> impl Future<Output = std::io::Result<Vec<u8>>> + Send + 'static;
}
```

L’implémentation disque clone seulement le sender, pas `DiskStorage` :

```rust
fn read(&mut self, offset: u64, len: usize)
    -> impl std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'static
{
    let tx = self.cmd_tx.clone();
    async move {
        let (reply, rx) = oneshot::channel();
        tx.send(DiskCommand::Read { offset, len, reply }).await
            .map_err(|_| std::io::Error::other("storage task closed"))?;
        rx.await.map_err(|_| std::io::Error::other("storage reply dropped"))?
    }
}
```

`restore()` continue d’utiliser `.read(...).await` sans changement. Aucun type Tokio dans le port, aucune contrainte `Clone`/`Sync` sur `S`.

## 5. `SwarmIO` : tâches de lecture et retour dans step

Ajouter des champs et initialiser `JoinSet::new()` / `HashMap::new()` :

```rust
use tokio::task::{AbortHandle, JoinSet};

type PieceReadResult = (PieceReadId, usize, std::io::Result<Vec<u8>>);
// reads: JoinSet<PieceReadResult>,
// read_handles: HashMap<PieceReadId, AbortHandle>,
```

Dans `handle_output()` :

```rust
Output::ReadPiece { id, piece_index, offset, len } => {
    let read = self.piece_store.read(offset, len);
    let handle = self.reads.spawn(async move { (id, piece_index, read.await) });
    self.read_handles.insert(id, handle);
},
Output::CancelPieceRead(id) => {
    if let Some(handle) = self.read_handles.remove(&id) {
        handle.abort(); // drop du oneshot receiver, pas du worker disque
    }
},
```

Dans le `select!` de `run()` :

```rust
result = self.reads.join_next(), if !self.reads.is_empty() => {
    let result = result.expect("non-empty JoinSet");
    let (id, piece_index, data) = match result {
        Ok(value) => value,
        Err(error) if error.is_cancelled() => continue,
        Err(error) => return Err(Error::ReadTask(error)),
    };
    if self.read_handles.remove(&id).is_none() {
        continue; // fini juste avant abort : résultat déjà annulé
    }
    let data = data?; // véritable erreur disque : propagée
    if !self.swarm.valid_stored_piece(piece_index, &data) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData, "invalid stored piece"
        ).into());
    }
    Input::PieceLoaded { id, piece_index, data }
},
```

Ajouter `Error::ReadTask(tokio::task::JoinError)` avec `#[error("piece read task failed: {0}")]` ; une panique n’est pas une annulation normale.

Ajouter dans `PieceManager` :

```rust
pub(super) fn valid_stored_piece(&self, index: usize, data: &[u8]) -> bool {
    self.pieces.get(index).is_some_and(|p| {
        p.is_complete()
            && data.len() == p.length
            && verify_piece_hash(self.metainfo.pieces[index], data)
    })
}
```

Puis `pub(crate) fn valid_stored_piece(&self, index: usize, data: &[u8]) -> bool` dans `Swarm`, déléguant à `self.pieces.valid_stored_piece(index, data)`. Une pièce modifiée/tronquée sur disque ne doit pas être uploadée. Ne pas remettre cette lecture dans `LocalPiece` : elle ne doit ni compter comme téléchargement ni provoquer Have/WritePiece/Completed.

Le `JoinSet` doit être drainé normalement, y compris les tâches annulées ; pas de tâches fire-and-forget. Déplacer la boucle actuelle dans `run_loop()` et centraliser le nettoyage :

```rust
pub async fn run(&mut self) -> Result<()> {
    self.restore().await?;
    let result = self.run_loop().await;
    self.reads.abort_all();
    while self.reads.join_next().await.is_some() {}
    self.read_handles.clear();
    result
}
```

Cela conserve l’erreur de la boucle et annule les lecteurs sans toucher aux écritures. Lors d’une annulation externe de la tâche SwarmIO, le drop de son `JoinSet` abort aussi les lecteurs. La limite de 8 concerne les lectures logiques actives ; les tâches annulées peuvent rester temporairement dans le JoinSet jusqu’à leur collecte.

Le worker reste séquentiel. Une seule lecture disque est physiquement active ; les autres tâches attendent la queue/réponse. Pas de nouveaux handles fichiers ni de lectures parallèles sur un curseur partagé.

## 6. `disk_storage.rs` : ignorer une lecture annulée encore en queue

Seule modification du bras Read :

```rust
DiskCommand::Read { offset, len, reply } => {
    if reply.is_closed() {
        continue;
    }
    let result = Self::read_from_files(&mut files, offset, len).await;
    let _ = reply.send(result);
},
```

Si Cancel arrive pendant `read_from_files()`, cette lecture finit et son résultat est abandonné. Ne pas interrompre les opérations seek/read au milieu du worker.

Conserver impérativement :

- les écritures en queue bornée avec backpressure ;
- le fail-fast du worker sur erreur d’écriture ;
- `Output::Completed` → `piece_store.flush().await?` avant publication du succès ;
- l’ordre `WritePiece` avant toute lecture ultérieure de cette pièce. Préférer `WritePiece` puis `Broadcast(Have)` dans les outputs de completion ; `SwarmIO` traite tous les outputs du step avant le prochain input.

## 7. Ordre de construction et vérification

Ces étapes sont des points de construction, pas des versions à livrer séparément : supprimer les buffers complets rend l’ancien upload inutilisable tant que le chemin disque n’est pas raccordé.

1. Enum d’état + compteurs + restoration : supprimer la RAM des pièces complètes.
2. Cache pondéré + validation des intervalles + configuration.
3. Future de lecture `'static` + JoinSet + ReadPiece/PieceLoaded, erreurs propagées.
4. Déduplication des lectures + attente bornée + Cancel + nettoyage des peers/statuts.
5. Vérifier hors dépôt, sans ajouter de tests au repository :

| Cas                                                      | Attendu                                                                                      |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Pièce valide puis éviction                               | Complete/bitfield/compteurs inchangés, données libérées                                      |
| Cache miss, puis hit                                     | une lecture initiale ; aucune lecture au hit                                                 |
| Deux peers demandent la même pièce                       | une lecture, deux réponses                                                                   |
| Cancel d’un seul demandeur                               | l’autre reçoit sa réponse                                                                    |
| Cancel du dernier, avant/après début IO                  | pas de réponse, pas d’insertion cache                                                        |
| Annuler A puis relire A ; ancien résultat arrive         | ancien ID ignoré                                                                             |
| Pièce plus grande que le budget / cache désactivé        | réponse correcte, pas de rétention cache                                                     |
| Offset overflow, len 0, >16 KiB, hors pièce/non vérifiée | aucune lecture ni réponse                                                                    |
| Déconnexion/remplacement/status sans upload              | attentes nettoyées, lectures inutiles annulées                                               |
| Disque tronqué, contenu modifié, erreur réelle           | erreur propagée, aucun upload corrompu                                                       |
| Restart torrent complet                                  | hashes vérifiés, aucune réécriture/redownload, RAM non proportionnelle aux données complètes |

API du cache : [Cache unsync](https://docs.rs/quick_cache/0.7.0/quick_cache/unsync/struct.Cache.html), [Weighter](https://docs.rs/quick_cache/0.7.0/quick_cache/trait.Weighter.html).
