# Pièces sur disque et lectures d’upload annulables

## Contrat retenu

- **Aucun cache applicatif.** Chaque demande déclenche une lecture de son intervalle exact via `PieceStore`. Le système d’exploitation gère son propre cache de fichiers.
- **SHA-1 uniquement au téléchargement et à la restauration.** Hypothèse : les fichiers ne sont pas modifiés extérieurement pendant la session. Pour l’upload, vérifier seulement les limites de la demande et la longueur lue.
- Une demande accepte un offset arbitraire et `0 < len ≤ 16 KiB`, sans dépasser la pièce. Pas d’alignement imposé, de découpage ni d’assemblage.
- Les demandes **identiques encore en attente** partagent une lecture. Les intervalles différents, même chevauchants, sont lus séparément.
- Cancel retire un peer pour l’intervalle exact demandé. Annuler le lecteur seulement s’il ne reste aucun demandeur. Une IO déjà commencée peut finir ; **ne jamais abort le worker disque**.
- Une réponse tardive/annulée est ignorée. Après émission de `SendToPeer`, Cancel ne retire pas le message déjà envoyé à la queue réseau.

Conserver le Sans-IO, `PieceStore`, sa queue FIFO bornée et le flush avant completion. Supprimer du précédent plan : dépendance `quick_cache`, budget/configuration de cache, blocs d’upload alignés, `PendingUpload`, buffers d’assemblage et helpers de copie d’intersections.

## 1. `Piece` et `PieceManager` : état vérifié sans données complètes

Cette partie est déjà engagée dans le code : conserver la nouvelle responsabilité de `Piece`.

```rust
struct Piece {
    length: usize,
    expected_hash: [u8; 20],
    state: PieceState,
}

enum PieceState {
    Partial { blocks: Box<[BlockState]>, received: usize },
    Complete,
}
```

`Piece::receive_block()` : valider index/taille, ignorer les doublons, stocker le bloc. Quand tous les blocs sont reçus, assembler et vérifier SHA-1 ; hash invalide → reset, hash valide → `Complete` et `Ok(Some(buffer))`. La transition libère les buffers des blocs. `Ok(None)` couvre doublon, réception incomplète et hash invalide.

`PieceManager::receive_block()` garde uniquement la coordination :

```rust
let block_index = block_ref.block_index()?;
let Some(buffer) = p.receive_block(block_index, data)? else {
    return Ok(None);
};
self.bitfield.set_bit(piece_index)?;
// Retourner CompletedPiece avec buffer et son offset dans le torrent.
```

Conserver les statistiques : `Piece::blocks_total()` calcule `length.div_ceil(BLOCK_SIZE)` ; `blocks_received()` retourne `received` pour Partial, et tous les blocs pour Complete. Les agrégations du manager et le scheduler utilisent ces méthodes.

Supprimer les anciennes `Piece::read()` / `PieceManager::read_block()` ; remplacer l’appel dans `Swarm::on_message_request()` par le chemin ci-dessous.

Garder `Input::LocalPiece` / `on_local_piece()` : restauration avec SHA-1, sans WritePiece ni compteur réseau. La RAM des pièces partielles et de la restauration reste hors des limites de l’upload.

### Validation d’un intervalle d’upload

Seule nouvelle méthode nécessaire dans `PieceManager` :

```rust
pub(super) fn valid_upload_range(&self, index: usize, offset: usize, len: usize) -> bool {
    self.pieces.get(index).is_some_and(|p| {
        p.is_complete()
            && len > 0
            && len <= BLOCK_SIZE
            && offset.checked_add(len).is_some_and(|end| end <= p.length)
    })
}
```

`BlockRef` reste interne au téléchargement : ne pas l’exposer pour les uploads. Pas de `upload_block_len()`, cache, setter ni commande de configuration.

## 2. `swarm.rs` : une seule map d’attente

Ajouter `HashMap` aux imports. Ne pas réutiliser `BlockAssignments` : il suit nos téléchargements, pas les demandes d’upload reçues.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadReadId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadRange {
    pub piece_index: usize,
    pub offset: usize, // relatif au début de la pièce, pas forcément aligné
    pub len: usize,
}

struct PendingRead {
    id: UploadReadId,
    peers: HashSet<SocketAddr>,
}

// Champs de Swarm, initialiser la map vide et le compteur à 0 :
// pending_reads: HashMap<UploadRange, PendingRead>,
// next_upload_read_id: u64,

// Output :
// ReadUpload { id: UploadReadId, range: UploadRange, offset: u64 },
// CancelUploadRead(UploadReadId),

// Input :
// UploadLoaded { id: UploadReadId, range: UploadRange, data: Vec<u8> },
```

Un ID identifie **une lecture**, pas uniquement son intervalle : une ancienne lecture annulée ne doit pas satisfaire une nouvelle demande identique.

### Request

Remplacer `on_message_request()` :

```rust
fn on_message_request(
    &mut self, piece_index: usize, piece_offset: usize, piece_len: usize, addr: SocketAddr,
) -> Vec<Output> {
    if !self.status.upload()
        || !self.pieces.valid_upload_range(piece_index, piece_offset, piece_len)
    {
        return vec![];
    }
    let range = UploadRange { piece_index, offset: piece_offset, len: piece_len };
    if self.pending_reads.get(&range).is_some_and(|r| r.peers.contains(&addr)) {
        return vec![]; // même demande déjà pending pour ce peer
    }
    let total: usize = self.pending_reads.values().map(|r| r.peers.len()).sum();
    let per_peer = self.pending_reads.values().filter(|r| r.peers.contains(&addr)).count();
    if total >= MAX_PENDING_UPLOADS || per_peer >= MAX_PENDING_PER_PEER {
        let mut out = self.on_disconnected(addr); // inclut cancel_peer_uploads
        out.push(Output::DisconnectPeer(addr));
        return out;
    }
    if let Some(read) = self.pending_reads.get_mut(&range) {
        read.peers.insert(addr);
        return vec![]; // demande identique, lecture partagée
    }
    let id = UploadReadId(self.next_upload_read_id);
    self.next_upload_read_id = self.next_upload_read_id.checked_add(1)
        .expect("upload read id exhausted");
    self.pending_reads.insert(range, PendingRead {
        id, peers: HashSet::from([addr]),
    });
    vec![Output::ReadUpload {
        id,
        range,
        offset: piece_index as u64 * self.metainfo.piece_length as u64 + piece_offset as u64,
    }]
}
```

Constantes de module : `MAX_PENDING_PER_PEER = 32`, `MAX_PENDING_UPLOADS = 256`. Compter les demandeurs, pas seulement les lectures partagées. Cela borne aussi le nombre de lectures distinctes à 256. Ne pas limiter les lectures à 8 et déconnecter dès la 9e demande : un peer normal peut envoyer une pipeline de 32 requêtes. La queue disque, elle, reste de capacité 8 avec backpressure.

La déconnexion à saturation est une politique simple, pas une preuve de violation du protocole. Elle nettoie l’attente et évite d’ignorer silencieusement des demandes acceptées. Les doublons sont traités avant les limites.

### UploadLoaded

Le bridge IO garantit la longueur exacte avant d’injecter cet Input :

```rust
fn on_upload_loaded(
    &mut self, id: UploadReadId, range: UploadRange, data: Vec<u8>,
) -> Vec<Output> {
    if !self.pending_reads.get(&range).is_some_and(|r| r.id == id) {
        return vec![]; // annulé / ancienne génération
    }
    let read = self.pending_reads.remove(&range).expect("checked above");
    let mut out = vec![];
    if !self.status.upload() {
        return out;
    }
    for addr in read.peers {
        if !self.peer_registry.contains_addr(addr) {
            continue;
        }
        self.uploaded_bytes += data.len() as u64;
        out.push(Output::SendToPeer {
            addr,
            message: Message::Piece {
                piece_index: range.piece_index,
                piece_offset: range.offset,
                data: data.clone(),
            },
        });
    }
    out
}
```

Les copies par peer suivent le modèle actuel de `Message::Piece` possédant son Vec. Les données ne sont pas conservées après émission. `uploaded_bytes` mesure l’émission domain, comme actuellement, pas une confirmation réseau.

### Cancel exact et nettoyage

```rust
fn cancel_upload(&mut self, addr: SocketAddr, range: UploadRange) -> Vec<Output> {
    let Some(read) = self.pending_reads.get_mut(&range) else { return vec![]; };
    read.peers.remove(&addr);
    if !read.peers.is_empty() {
        return vec![];
    }
    let id = read.id;
    self.pending_reads.remove(&range);
    vec![Output::CancelUploadRead(id)]
}

fn cancel_peer_uploads(&mut self, addr: SocketAddr) -> Vec<Output> {
    let mut out = vec![];
    self.pending_reads.retain(|_, read| {
        read.peers.remove(&addr);
        if read.peers.is_empty() {
            out.push(Output::CancelUploadRead(read.id));
            false
        } else {
            true
        }
    });
    out
}

fn cancel_all_uploads(&mut self) -> Vec<Output> {
    self.pending_reads.drain()
        .map(|(_, read)| Output::CancelUploadRead(read.id)).collect()
}
```

Router `Message::Cancel { piece_index, piece_offset, piece_len }` vers `cancel_upload(addr, UploadRange { piece_index, offset: piece_offset, len: piece_len })`. Offset **et longueur** doivent correspondre. Un Cancel n’affecte jamais les autres peers.

Raccordements obligatoires :

- `step()` : `Input::UploadLoaded` → `on_upload_loaded()`.
- `on_disconnected()` : retourner `cancel_peer_uploads(addr)` après nettoyage registry/assignments.
- `on_connected()` : `out.extend(self.on_disconnected(old_addr))`, ne plus ignorer ces outputs.
- Déconnexion décidée localement (protocole, saturation, remplacement, etc.) : nettoyer avant `DisconnectPeer`, sans attendre l’événement réseau. Cela protège aussi une reconnexion au même SocketAddr.
- `SetStatus` : mettre à jour le statut puis, si upload désactivé, retourner `cancel_all_uploads()`. `UploadOnly` conserve les attentes ; `DownloadOnly` et `Stopped` les annulent.

## 3. `PieceStore` : future de lecture détachable

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

## 4. `SwarmIO` : tâches annulables et résultat dans step

```rust
use tokio::task::{AbortHandle, JoinSet};

type UploadReadResult = (UploadReadId, UploadRange, std::io::Result<Vec<u8>>);
// reads: JoinSet<UploadReadResult>, initialisé par JoinSet::new()
// read_handles: HashMap<UploadReadId, AbortHandle>, initialisé vide
```

Importer les nouveaux types depuis `domain::swarm`. Dans `handle_output()` :

```rust
Output::ReadUpload { id, range, offset } => {
    let read = self.piece_store.read(offset, range.len);
    let handle = self.reads.spawn(async move { (id, range, read.await) });
    self.read_handles.insert(id, handle);
},
Output::CancelUploadRead(id) => {
    if let Some(handle) = self.read_handles.remove(&id) {
        handle.abort(); // drop du oneshot receiver, pas du worker disque
    }
},
```

Dans le `select!` de la boucle :

```rust
result = self.reads.join_next(), if !self.reads.is_empty() => {
    let (id, range, data) = match result.expect("non-empty JoinSet") {
        Ok(value) => value,
        Err(error) if error.is_cancelled() => continue,
        Err(error) => return Err(Error::ReadTask(error)),
    };
    if self.read_handles.remove(&id).is_none() {
        continue; // fini avant abort mais déjà annulé logiquement
    }
    let data = data?; // véritable erreur disque : propagée
    if data.len() != range.len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof, "incomplete upload read"
        ).into());
    }
    Input::UploadLoaded { id, range, data }
},
```

Ajouter `Error::ReadTask(tokio::task::JoinError)` avec `#[error("upload read task failed: {0}")]`. Une panique n’est pas une annulation normale. Pas de rehash ; une modification extérieure de contenu à longueur identique ne sera pas détectée. Ne pas réinjecter ces données dans `LocalPiece` / `receive_block()` : ni réception réseau, ni SHA-1, ni Have/WritePiece/Completed.

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

Drainer aussi les tâches annulées pendant la boucle. Elles restent temporairement dans JoinSet jusqu’à collecte. Le drop du JoinSet abort ses lecteurs quand le coordinateur est détruit. Pas de tâches fire-and-forget.

Les lectures d’upload font chacune au plus 16 KiB ; au plus 256 résultats actifs peuvent retenir environ 4 MiB de données, hors copies dans les queues réseau et tâches annulées en cours de collecte. Les limites ne bornent pas toute la RAM du client. Le worker séquentiel peut devenir un goulot d’étranglement ; ne changer ce chemin que sur mesure.

## 5. `disk_storage.rs` : sauter les lectures annulées encore en queue

```rust
DiskCommand::Read { offset, len, reply } => {
    if reply.is_closed() {
        continue;
    }
    let result = Self::read_from_files(&mut files, offset, len).await;
    let _ = reply.send(result);
},
```

Si Cancel arrive pendant `read_from_files()`, laisser finir puis abandonner le résultat. Le worker reste séquentiel : pas de lectures parallèles sur un curseur partagé, pas d’interruption au milieu d’un seek/read. Les lectures existantes savent traverser plusieurs fichiers du torrent.

Conserver impérativement :

- écritures en queue bornée avec backpressure et fail-fast sur erreur ;
- `Output::Completed` → `piece_store.flush().await?` avant publication du succès ;
- `WritePiece` en queue avant toute lecture ultérieure de cette pièce. Préférer `WritePiece` puis `Broadcast(Have)` : SwarmIO traite tous les outputs d’un step avant le prochain input.

## 6. Construction et vérification

Les étapes sont des points de construction, pas des versions indépendantes à livrer : supprimer les buffers complets casse l’ancien upload tant que le chemin disque n’est pas raccordé.

1. Conserver la nouvelle implémentation de Piece et ses compteurs ; ajouter `valid_upload_range()`.
2. Future `'static` + JoinSet + ReadUpload/UploadLoaded, erreurs/lectures courtes propagées.
3. Une map de lectures partagées, bornes, Cancel et nettoyage lifecycle.
4. Vérifier **hors dépôt**, sans ajouter de tests au repository :

| Cas                                                          | Attendu                                               |
| ------------------------------------------------------------ | ----------------------------------------------------- |
| Pièce complète                                               | données libérées, bitfield/compteurs inchangés        |
| Demande valide                                               | lecture exacte, réponse de la longueur demandée       |
| Même demande après réponse précédente                        | nouvelle lecture, aucune rétention applicative        |
| Deux peers demandent le même intervalle pending              | une lecture, deux réponses                            |
| Intervalles chevauchants différents                          | lectures distinctes, réponses exactes                 |
| Demande non alignée / traversant deux blocs ou deux fichiers | lecture exacte, sans assemblage domain                |
| Cancel d’un seul demandeur                                   | l’autre reçoit sa réponse                             |
| Cancel du dernier avant/pendant IO                           | aucune réponse, lecteur abandonné                     |
| Annuler A puis relire A ; ancien résultat arrive             | ancien ID ignoré                                      |
| Cancel avec mauvais offset/len/peer                          | aucune autre demande supprimée                        |
| Dernier intervalle court                                     | longueur correcte sur réseau                          |
| Offset overflow, len 0, >16 KiB, hors pièce/non vérifiée     | aucune IO ni réponse                                  |
| Pipeline de 32 demandes distinctes d’un peer                 | acceptée sans limite artificielle de 8 lectures       |
| Déconnexion/remplacement/status sans upload                  | attentes nettoyées, lecteurs inutiles annulés         |
| Lecture courte / erreur disque / panique lecteur             | erreur propagée, aucune réponse incomplète            |
| Grande pièce                                                 | lecture d’upload ≤16 KiB, aucun rehash                |
| Restart torrent complet                                      | SHA-1 de restauration, aucun redownload ni réécriture |
