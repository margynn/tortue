# Pièces sur disque, cache de blocs et Cancel

## Contrat retenu

- **SHA-1 uniquement au téléchargement et à la restauration.** Une pièce complète est fiable ; pas de rehash pour l’upload. Hypothèse : les fichiers ne sont pas modifiés extérieurement pendant la session.
- Les pièces partielles gardent leurs blocs en RAM. Les pièces complètes gardent seulement état/longueur ; leurs données sont déplacées dans `WritePiece`.
- Cache de **blocs canoniques de 16 KiB**, clé `(piece_index, piece_offset aligné)`. Dernier bloc d’une pièce éventuellement plus court.
- Une requête réseau valide peut être non alignée et chevaucher deux blocs. Lire seulement les blocs absents et assembler l’intervalle demandé.
- **Une lecture partagée par bloc**, pas par pièce. Cancel retire une demande exacte ; annuler une lecture seulement quand elle n’a plus de demandeurs.
- Une lecture annulée/tardive ne remplit pas le cache et ne produit aucune réponse. Un Cancel après émission de `SendToPeer` ne fait rien ; pas de purge des blocs déjà cachés.
- Annuler abandonne le résultat et évite une IO encore en queue ; une IO commencée peut finir. **Ne jamais abort le worker disque.**

Conserver `PieceStore`, la queue FIFO bornée et le flush de completion. Pas de nouvelle abstraction de stockage.

## 1. `piece_manager.rs` : supprimer les données des pièces complètes

Conserver la longueur hors de l’enum :

```rust
struct Piece {
    length: usize,
    state: PieceState,
}

enum PieceState {
    Partial { blocks: Vec<BlockState>, received: usize },
    Complete,
}
```

| Méthode               | Partial                       | Complete                                  |
| --------------------- | ----------------------------- | ----------------------------------------- |
| `is_complete()`       | false                         | true                                      |
| nombre de blocs       | `length.div_ceil(BLOCK_SIZE)` | idem                                      |
| blocs reçus           | `received`                    | nombre de blocs                           |
| `unreceived_blocks()` | blocs Missing                 | aucun                                     |
| `is_partial()`        | `received > 0`                | false                                     |
| `receive_block()`     | comportement actuel           | `Ok(false)` après validation index/taille |
| `buffer()`            | concaténer si tous reçus      | inutile                                   |
| `reset()`             | recréer les blocs Missing     | idem si nécessaire                        |

Aujourd’hui `is_complete()` signifie aussi « tous les blocs reçus avant SHA-1 ». Introduire `all_blocks_received()` pour cette étape intermédiaire :

```rust
// Dans PieceManager::receive_block(), après validation de BlockRef :
if !p.receive_block(block_index, data)? || !p.all_blocks_received() {
    return Ok(None);
}
let buffer = p.buffer().expect("all blocks received");
if !verify_piece_hash(self.metainfo.pieces[piece_index], &buffer) {
    p.reset();
    self.bitfield.unset_bit(piece_index)?;
    return Ok(None);
}
p.state = PieceState::Complete; // libère les buffers des blocs
self.bitfield.set_bit(piece_index)?;
// Retourner CompletedPiece comme aujourd’hui, en déplaçant buffer.
```

Adapter `blocks_total()`, `blocks_received()`, `available_bytes()` et le scheduler aux méthodes, pas aux anciens champs. `buffer()` doit utiliser `all_blocks_received()`, pas le nouvel `is_complete()`.

Garder `Input::LocalPiece` / `on_local_piece()` : restauration validée sans WritePiece ni compteur réseau. Une pièce invalide reste à télécharger. **Cette restauration et la réception SHA-1 restent à l’échelle de la pièce** : le nouveau cache borne l’upload, pas la RAM des téléchargements/restaurations.

## 2. Clés, intervalles et cache de 50 MiB

Réutiliser le `BlockRef` existant, pas une seconde clé équivalente. Pour l’exposer dans les Input/Output publics : rendre `BlockRef` et ses champs `pub`, puis `pub use piece_manager::BlockRef` dans `swarm.rs` (retirer son import privé). Garder `block_index()` privé. Importer `BLOCK_SIZE` dans `swarm.rs` avec une visibilité `pub(super)` dans `piece_manager.rs`.

Une requête d’upload n’a pas besoin d’être alignée :

```rust
pub(super) fn valid_upload_range(&self, index: usize, offset: usize, len: usize) -> bool {
    self.pieces.get(index).is_some_and(|p| {
        p.is_complete() && len > 0 && len <= BLOCK_SIZE
            && offset.checked_add(len).is_some_and(|end| end <= p.length)
    })
}

pub(super) fn upload_block_len(&self, block: BlockRef) -> Option<usize> {
    let piece = self.pieces.get(block.piece_index)?;
    if !piece.is_complete()
        || !block.piece_offset.is_multiple_of(BLOCK_SIZE)
        || block.piece_offset >= piece.length
    {
        return None;
    }
    Some((piece.length - block.piece_offset).min(BLOCK_SIZE))
}
```

Ajouter `quick_cache = "0.7"` à `tortue_lib/Cargo.toml`. Cache `unsync`, le domaine étant manipulé par une seule boucle :

```rust
use quick_cache::{unsync::Cache, Weighter};

const DEFAULT_UPLOAD_CACHE_BYTES: usize = 50 * 1024 * 1024;

struct BlockWeighter;
impl Weighter<BlockRef, Vec<u8>> for BlockWeighter {
    fn weight(&self, _: &BlockRef, data: &Vec<u8>) -> u64 {
        data.capacity() as u64
    }
}

// PieceManager :
// cache: Cache<BlockRef, Vec<u8>, BlockWeighter>,
// cache_bytes: usize, initialisé à DEFAULT_UPLOAD_CACHE_BYTES

let cache = Cache::with_weighter(
    DEFAULT_UPLOAD_CACHE_BYTES / BLOCK_SIZE,
    DEFAULT_UPLOAD_CACHE_BYTES as u64,
    BlockWeighter,
);
```

Méthodes de `PieceManager` :

```rust
pub(super) fn cached_block(&self, block: BlockRef) -> Option<&[u8]> {
    self.cache.get(&block).map(Vec::as_slice)
}

pub(super) fn cache_block(&mut self, block: BlockRef, data: Vec<u8>) {
    // 0 désactive ; refuser une entrée dépassant le budget.
    if self.cache_bytes > 0 && data.capacity() <= self.cache_bytes {
        self.cache.insert(block, data);
    }
}

pub(super) fn set_cache_bytes(&mut self, bytes: usize) {
    self.cache_bytes = bytes;
    self.cache.set_capacity(bytes as u64);
}
```

Configurer via `SwarmCommand::SetUploadCacheBytes(usize)` → `self.pieces.set_cache_bytes(bytes)`. Exposer dans `SwarmHandle` :

```rust
pub async fn set_upload_cache_bytes(&self, bytes: usize) -> Result<()> {
    self.send(SwarmCommand::SetUploadCacheBytes(bytes)).await
}
```

Pas de modification des constructeurs ni de CLI. Remplir le cache seulement sur lecture disque, pas lors de `CompletedPiece` : pas de clone de pièce pour cache + écriture. L’éviction ne modifie jamais le bitfield.

50 MiB ≈ **3 200 blocs** de 16 KiB, hors métadonnées du cache. Le budget ne couvre pas pièces partielles, écritures en queue, assemblages pending ou messages réseau.

## 3. `swarm.rs` : lectures partagées et assemblage pending

Ne pas réutiliser `BlockAssignments` : il suit nos téléchargements, pas les demandes d’upload reçues.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockReadId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UploadRequest {
    addr: SocketAddr,
    piece_index: usize,
    offset: usize,
    len: usize,
}

struct PendingUpload {
    data: Vec<u8>, // exactement request.len ; fragments déjà disponibles copiés ici
    missing: HashSet<BlockRef>,
}

struct PendingBlockRead {
    id: BlockReadId,
    requests: HashSet<UploadRequest>,
}

// Swarm, initialiser les maps vides et le compteur à 0 :
// pending_uploads: HashMap<UploadRequest, PendingUpload>,
// pending_reads: HashMap<BlockRef, PendingBlockRead>,
// next_block_read_id: u64,

// Output :
// ReadBlock { id: BlockReadId, block: BlockRef, offset: u64, len: usize },
// CancelBlockRead(BlockReadId),

// Input :
// BlockLoaded { id: BlockReadId, block: BlockRef, data: Vec<u8> },
```

ID **par lecture**, pas par bloc : une ancienne lecture annulée ne doit pas satisfaire une nouvelle demande du même bloc.

### Découper une requête et copier les intersections

Deux helpers privés dans `swarm.rs`. Précondition : requête validée par `valid_upload_range()` ; donc `offset + len` ne déborde pas et touche au plus deux blocs.

```rust
fn request_blocks(request: UploadRequest) -> Vec<BlockRef> {
    let first = request.offset / BLOCK_SIZE * BLOCK_SIZE;
    (first..request.offset + request.len).step_by(BLOCK_SIZE)
        .map(|piece_offset| BlockRef { piece_index: request.piece_index, piece_offset })
        .collect()
}

fn copy_fragment(request: UploadRequest, out: &mut [u8], block: BlockRef, data: &[u8]) {
    let start = request.offset.max(block.piece_offset);
    let end = (request.offset + request.len).min(block.piece_offset + data.len());
    out[start - request.offset..end - request.offset]
        .copy_from_slice(&data[start - block.piece_offset..end - block.piece_offset]);
}
```

### Request : chemin complet

Dans `on_message_request()` : refuser upload désactivé / intervalle invalide, puis construire `UploadRequest`. Un doublon déjà pending ne crée rien.

```rust
let request = UploadRequest { addr, piece_index, offset: piece_offset, len: piece_len };
if self.pending_uploads.contains_key(&request) {
    return vec![];
}
let blocks = request_blocks(request);
let missing: HashSet<_> = blocks.iter().copied()
    .filter(|b| self.pieces.cached_block(*b).is_none()).collect();
let new_reads = missing.iter().filter(|b| !self.pending_reads.contains_key(*b)).count();

// Constantes de module : 8 lectures actives, 32 pending/peer, 256 pending au total.
if !missing.is_empty() && (
    self.pending_reads.len() + new_reads > MAX_BLOCK_READS
    || self.pending_uploads.len() >= MAX_PENDING_UPLOADS
    || self.pending_uploads.keys().filter(|r| r.addr == addr).count() >= MAX_PENDING_PER_PEER
) {
    let mut out = self.on_disconnected(addr); // inclut cancel_peer_uploads
    out.push(Output::DisconnectPeer(addr));
    return out;
}

let mut data = vec![0; request.len];
for block in &blocks {
    if let Some(cached) = self.pieces.cached_block(*block) {
        copy_fragment(request, &mut data, *block, cached);
    }
}
if missing.is_empty() {
    return self.send_upload(request, data);
}

let mut out = vec![];
for block in &missing {
    if let Some(read) = self.pending_reads.get_mut(block) {
        read.requests.insert(request);
        continue;
    }
    let id = BlockReadId(self.next_block_read_id);
    self.next_block_read_id = self.next_block_read_id.checked_add(1)
        .expect("block read id exhausted");
    self.pending_reads.insert(*block, PendingBlockRead {
        id, requests: HashSet::from([request]),
    });
    out.push(Output::ReadBlock {
        id,
        block: *block,
        offset: piece_index as u64 * self.metainfo.piece_length as u64
            + block.piece_offset as u64,
        len: self.pieces.upload_block_len(*block).expect("validated range"),
    });
}
self.pending_uploads.insert(request, PendingUpload { data, missing });
out
```

Définir `MAX_BLOCK_READS = 8`, `MAX_PENDING_PER_PEER = 32`, `MAX_PENDING_UPLOADS = 256`. Les gardes précédentes sont avant toute allocation/lecture ; les cache hits restent servis directement. Au pire, les assemblages pending occupent **4 MiB** (256 × 16 KiB), même si le cache est désactivé.

Ajouter le petit helper `send_upload(request, data) -> Vec<Output>` : incrémenter `uploaded_bytes` de `data.len()` puis produire le `SendToPeer { message: Message::Piece { piece_index, piece_offset: request.offset, data }, addr }` existant. Ces compteurs mesurent l’émission domain, comme actuellement, pas une confirmation réseau.

### BlockLoaded

```rust
fn on_block_loaded(&mut self, id: BlockReadId, block: BlockRef, data: Vec<u8>) -> Vec<Output> {
    if !self.pending_reads.get(&block).is_some_and(|read| read.id == id) {
        return vec![]; // ancienne génération / annulation : ni réponse ni cache
    }
    let read = self.pending_reads.remove(&block).expect("checked above");
    let mut out = vec![];
    for request in read.requests {
        let Some(upload) = self.pending_uploads.get_mut(&request) else { continue; };
        copy_fragment(request, &mut upload.data, block, &data);
        upload.missing.remove(&block);
        if upload.missing.is_empty() {
            let upload = self.pending_uploads.remove(&request).expect("pending upload");
            if self.status.upload() && self.peer_registry.contains_addr(request.addr) {
                out.extend(self.send_upload(request, upload.data));
            }
        }
    }
    self.pieces.cache_block(block, data);
    out
}
```

**Copier dans les assemblages AVANT d’insérer dans le cache.** Sinon un cache de 0 ou 16 KiB pourrait provoquer des relectures infinies pour une requête chevauchant deux blocs : chaque arrivée évincerait l’autre. Une fois copié, un fragment n’a plus besoin de rester dans le cache.

### Cancel exact et nettoyage

```rust
fn cancel_upload(&mut self, request: UploadRequest) -> Vec<Output> {
    let Some(upload) = self.pending_uploads.remove(&request) else { return vec![]; };
    let mut out = vec![];
    for block in upload.missing {
        let Some(read) = self.pending_reads.get_mut(&block) else { continue; };
        read.requests.remove(&request);
        if read.requests.is_empty() {
            let id = read.id;
            self.pending_reads.remove(&block);
            out.push(Output::CancelBlockRead(id));
        }
    }
    out
}

fn cancel_peer_uploads(&mut self, addr: SocketAddr) -> Vec<Output> {
    let requests: Vec<_> = self.pending_uploads.keys().copied()
        .filter(|r| r.addr == addr).collect();
    requests.into_iter().flat_map(|r| self.cancel_upload(r)).collect()
}

fn cancel_all_uploads(&mut self) -> Vec<Output> {
    self.pending_uploads.clear();
    self.pending_reads.drain()
        .map(|(_, read)| Output::CancelBlockRead(read.id)).collect()
}
```

Router `Message::Cancel { piece_index, piece_offset, piece_len }` vers `cancel_upload(UploadRequest { addr, piece_index, offset: piece_offset, len: piece_len })` : mêmes peer, pièce, offset **et longueur**. Un Cancel n’annule jamais les demandes d’autres peers.

Raccordements obligatoires :

- `step()` : `Input::BlockLoaded` → `on_block_loaded()`.
- `on_disconnected()` : retourner les outputs de `cancel_peer_uploads(addr)` après nettoyage registry/assignments.
- `on_connected()` : `out.extend(self.on_disconnected(old_addr))`, ne plus ignorer ces outputs.
- Déconnexion décidée localement (violation de protocole, limite, remplacement, etc.) : nettoyer avant `DisconnectPeer`, sans attendre un événement réseau. Cela protège aussi une reconnexion au même SocketAddr.
- `SetStatus` : mettre le statut à jour ; si upload désactivé, retourner `cancel_all_uploads()`. `UploadOnly` conserve les attentes ; `DownloadOnly` et `Stopped` les annulent.

## 4. `PieceStore` : future de lecture détachable

Ne pas attendre la lecture dans `handle_output()` : la boucle ne pourrait plus traiter Cancel. L’actuelle `async fn read(&mut self, ...)` garde un emprunt incompatible avec une tâche détachée. Modifier seulement `read` :

```rust
use std::future::Future;

pub trait PieceStore: Send {
    async fn write(&mut self, offset: u64, data: Vec<u8>) -> std::io::Result<()>;
    async fn flush(&mut self) -> std::io::Result<()>;
    fn read(&mut self, offset: u64, len: usize)
        -> impl Future<Output = std::io::Result<Vec<u8>>> + Send + 'static;
}
```

Dans `DiskStorage`, capturer uniquement un clone du sender :

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

La restauration continue d’appeler `.read(...).await`. Aucun type Tokio dans le port, aucune contrainte Clone/Sync sur `S`.

## 5. `SwarmIO` : lancer les lectures et restituer les résultats

```rust
use tokio::task::{AbortHandle, JoinSet};

type BlockReadResult = (BlockReadId, BlockRef, usize, std::io::Result<Vec<u8>>);
// reads: JoinSet<BlockReadResult>, initialisé par JoinSet::new()
// read_handles: HashMap<BlockReadId, AbortHandle>, initialisé vide
```

Importer les nouveaux types depuis `domain::swarm`. Dans `handle_output()` :

```rust
Output::ReadBlock { id, block, offset, len } => {
    let read = self.piece_store.read(offset, len);
    let handle = self.reads.spawn(async move { (id, block, len, read.await) });
    self.read_handles.insert(id, handle);
},
Output::CancelBlockRead(id) => {
    if let Some(handle) = self.read_handles.remove(&id) {
        handle.abort(); // drop du oneshot receiver, pas du worker disque
    }
},
```

Dans le `select!` de la boucle :

```rust
result = self.reads.join_next(), if !self.reads.is_empty() => {
    let (id, block, expected_len, data) = match result.expect("non-empty JoinSet") {
        Ok(value) => value,
        Err(error) if error.is_cancelled() => continue,
        Err(error) => return Err(Error::ReadTask(error)),
    };
    if self.read_handles.remove(&id).is_none() {
        continue; // résultat fini avant abort mais déjà annulé logiquement
    }
    let data = data?; // véritable erreur disque : propagée
    if data.len() != expected_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof, "incomplete stored block"
        ).into());
    }
    Input::BlockLoaded { id, block, data }
},
```

Ajouter `Error::ReadTask(tokio::task::JoinError)` avec `#[error("block read task failed: {0}")]`. Pas de rehash : **seulement contrôle de longueur**. Une modification de contenu à longueur identique n’est pas détectée, conformément à l’hypothèse retenue. Ne pas réinjecter ces données dans `LocalPiece` / `receive_block()` : ni réception réseau, ni SHA-1, ni Have/WritePiece/Completed.

Déplacer la boucle actuelle dans `run_loop()` et centraliser le nettoyage :

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

Drainer les tâches annulées aussi pendant la boucle. La limite de 8 porte sur les lectures logiques actives ; les tâches annulées restent temporairement dans JoinSet jusqu’à collecte. Le drop de JoinSet abort les lecteurs si le coordinateur est détruit. Ne pas lancer de tâches fire-and-forget.

## 6. `disk_storage.rs` : ignorer une lecture annulée encore en queue

```rust
DiskCommand::Read { offset, len, reply } => {
    if reply.is_closed() {
        continue;
    }
    let result = Self::read_from_files(&mut files, offset, len).await;
    let _ = reply.send(result);
},
```

Si Cancel arrive pendant `read_from_files()`, laisser la lecture finir et abandonner son résultat. Le worker reste séquentiel : pas de lectures parallèles sur le même curseur, pas d’interruption au milieu d’un seek/read.

Conserver impérativement :

- écritures en queue bornée avec backpressure et fail-fast sur erreur ;
- `Output::Completed` → `piece_store.flush().await?` avant publication du succès ;
- `WritePiece` en queue avant toute lecture ultérieure de ses blocs. Préférer `WritePiece` puis `Broadcast(Have)` : SwarmIO traite tous les outputs d’un step avant le prochain input.

## 7. Construction et vérification

Ces étapes sont des points de construction, pas des versions indépendantes à livrer : supprimer les buffers complets casse l’ancien upload tant que le chemin disque n’est pas raccordé.

1. Enum d’état + compteurs + restauration, SHA-1 initial inchangé.
2. Cache de blocs + intervalles/assemblage + configuration.
3. Future `'static` + JoinSet + ReadBlock/BlockLoaded, erreurs et lectures courtes propagées.
4. Lectures partagées + bornes + Cancel + nettoyage lifecycle.
5. Vérifier **hors dépôt**, sans ajouter de tests au repository :

| Cas                                                      | Attendu                                               |
| -------------------------------------------------------- | ----------------------------------------------------- |
| Pièce validée puis éviction                              | Complete/bitfield/compteurs inchangés                 |
| Cache miss puis hit                                      | une lecture de bloc, aucune au hit                    |
| Deux peers demandent le même bloc                        | une lecture, deux réponses                            |
| Requête non alignée chevauchant deux blocs               | assemblage exact, lectures des seuls blocs absents    |
| Deux blocs nécessaires, cache 0 ou 16 KiB                | réponse sans relectures en boucle                     |
| Cancel d’un seul demandeur                               | l’autre reçoit sa réponse                             |
| Cancel du dernier avant/pendant IO                       | pas de réponse ni nouvelle insertion cache            |
| Un fragment reçu puis Cancel                             | assemblage libéré ; fragment déjà caché conservé      |
| Annuler A puis relire A ; ancien résultat arrive         | ancien ID ignoré                                      |
| Dernier bloc court                                       | longueur correcte en cache et sur réseau              |
| Offset overflow, len 0, >16 KiB, hors pièce/non vérifiée | aucune IO ni réponse                                  |
| Déconnexion/remplacement/status sans upload              | attentes nettoyées, lectures inutiles annulées        |
| Lecture courte / erreur disque                           | erreur propagée, aucun bloc incomplet servi           |
| Grande pièce                                             | chaque lecture d’upload reste ≤16 KiB ; pas de rehash |
| Restart torrent complet                                  | SHA-1 de restauration, aucun redownload ni réécriture |

[Cache unsync](https://docs.rs/quick_cache/0.7.0/quick_cache/unsync/struct.Cache.html) · [Weighter](https://docs.rs/quick_cache/0.7.0/quick_cache/trait.Weighter.html)
