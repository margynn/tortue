# Reprise d’un téléchargement : restauration des pièces locales

## Objectif

Au démarrage, Tortue vérifie les données déjà présentes sur disque et restaure les pièces valides dans le swarm. Une pièce valide n’est pas retéléchargée ; une pièce absente, incomplète ou corrompue reste à télécharger.

La reprise ne repose pas sur la seule taille des fichiers : les octets de chaque pièce sont comparés au SHA-1 indiqué dans le metainfo.

## Flux de démarrage

```text
application/download.rs::start_download
    │
    ├─ Ouvre/crée les fichiers via DiskStorage::new
    ├─ Construit SwarmIO, qui possède un Swarm
    │
    ├─ Attend SwarmIO::restore()
    │     │
    │     └─ Pour chaque pièce :
    │           calcul de l’offset et de la longueur
    │           → PieceStore::read(offset, len)
    │           → DiskStorage : lecture des fichiers concernés
    │           → Swarm::restore_piece(index, data)
    │           → PieceManager : vérification SHA-1 et restauration
    │
    ├─ Publie l’état restauré et met à jour les statistiques tracker
    ├─ Démarre les trackers
    └─ Lance SwarmIO::run()
```

`start_download` attend donc la vérification avant de retourner le `Download`. Les trackers ne sont pas lancés pendant cette phase, ce qui évite une première annonce avec un `left` incorrect.

## 1. Port de stockage : lire sans dépendre du disque concret

**Fichier :** `tortue_lib/src/application/ports/piece_store.rs`

Le port expose désormais une lecture asynchrone en plus de l’écriture :

```rust
async fn read(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>>;
```

- `offset` est relatif au contenu global du torrent, pas à un fichier individuel.
- Le résultat contient jusqu’à `len` octets.
- Une lecture courte indique une plage incomplète.
- Une erreur IO réelle est remontée : elle ne devient pas silencieusement une pièce manquante.

Le domaine ne dépend ni de `File`, ni de Tokio. Seul l’orchestrateur appelle ce port pour obtenir les octets à vérifier.

## 2. DiskStorage : un worker pour les lectures et les écritures

**Fichier :** `tortue_lib/src/adapters/disk_storage.rs`

### Commandes du worker

Le canal qui transportait seulement `(offset, data)` transporte désormais un enum privé :

```text
Command::Write { offset, data }
Command::Read  { offset, len, reply }
```

La tâche existante `writer_task` traite les deux variantes. Son nom historique demeure, mais elle joue maintenant le rôle de worker de stockage.

Les fichiers ouverts restent possédés par cette seule tâche. Cela évite de dupliquer les handles ou de partager leurs positions de lecture/écriture derrière un mutex.

### Réponse à une lecture

`DiskStorage::read` :

1. Crée un canal `oneshot`.
2. Envoie `Command::Read`, avec le sender de réponse.
3. Attend la réponse du worker.
4. Retourne les données ou l’erreur IO.

Le canal `mpsc` sert à soumettre les opérations ; le `oneshot` sert à récupérer le résultat de cette lecture précise. Si le worker disparaît, la lecture retourne une erreur.

### Lecture à travers plusieurs fichiers

`read_from_files` calcule l’intersection entre la plage demandée et chaque `OutputFile` :

```text
plage demandée : [offset, offset + len)
plage fichier : [file.offset, file.offset + file.length)
intersection  : [max(des débuts), min(des fins))
```

Pour chaque intersection non vide, le worker :

- se positionne à l’offset local dans le fichier ;
- lit au maximum la longueur de l’intersection ;
- ajoute les octets au buffer global ;
- arrête la lecture si le fichier fournit moins d’octets que prévu.

Les fins de plage sont calculées avec `checked_add` pour détecter les débordements.

### Exemple

```text
Fichier A : abc       — offset 0, longueur 3
Fichier B : defghi    — offset 3, longueur 6

Contenu logique du torrent : abcdefghi
Taille des pièces : 4

Pièce 0 : abcd → trois octets de A, un octet de B
Pièce 1 : efgh → quatre octets de B
Pièce 2 : i    → dernière pièce plus courte
```

La vérification doit porter sur `abcd`, et non vérifier `abc` et `d` séparément : les hashes sont ceux des pièces, pas des fichiers.

### Ouverture des fichiers existants

L’ouverture conserve la politique actuelle :

- nouveau fichier : création exclusive et dimensionnement ;
- fichier existant régulier de taille inférieure ou égale : ouverture et extension éventuelle à la taille attendue ;
- fichier plus grand : refus.

L’extension se lit comme des zéros ; elle ne restaure pas les données perdues. La vérification SHA-1 détermine ensuite si les pièces sont valides.

La lecture de restauration elle-même ne réécrit pas les pièces. L’ouverture peut toutefois avoir créé ou agrandi les fichiers avant cette lecture.

## 3. PieceManager : restaurer les données validées

**Fichier :** `tortue_lib/src/domain/swarm/piece_manager.rs`

La méthode `restore_piece(piece_index, data)` est distincte de `receive_block`.

Elle :

1. Vérifie que l’index de pièce existe.
2. Vérifie que la longueur correspond à celle de la pièce, y compris la dernière.
3. Réutilise `verify_piece_hash` pour comparer le SHA-1 aux métadonnées.
4. Si le hash est valide, découpe les données en blocs de 16 KiB.
5. Réutilise `Piece::receive_block` pour remplir les blocs et leur état.
6. Positionne le bit de disponibilité de la pièce.

### Résultats

| Résultat    | Signification                                            |
| ----------- | -------------------------------------------------------- |
| `Ok(true)`  | Pièce valide restaurée                                   |
| `Ok(false)` | Hash incorrect ; pièce non restaurée                     |
| `Err(...)`  | Index ou longueur invalide, ou autre erreur de cohérence |

Ce chemin est destiné à l’initialisation d’un manager neuf, avant les échanges réseau. Un mismatch au démarrage laisse donc la pièce manquante.

### Pourquoi ne pas seulement charger le bitfield ?

Dans le modèle actuel, les uploads lisent les blocs depuis `PieceManager`, en mémoire. Il faut donc restaurer les octets des pièces valides, pas seulement déclarer qu’elles existent.

### Pourquoi ne pas utiliser le chemin réseau ?

`PieceManager::receive_block` comptabilise les données reçues. Le chemin réseau dans `Swarm` contrôle également les assignments et peut produire `WritePiece`, `Have` et `Completed`.

La restauration ne doit déclencher aucun de ces effets : elle remet en mémoire des données déjà sur disque, sans les compter comme trafic téléchargé.

## 4. Swarm : garder PieceManager encapsulé

**Fichier :** `tortue_lib/src/domain/swarm.rs`

`Swarm::restore_piece` délègue à son `PieceManager` privé. L’orchestrateur n’accède pas aux blocs ou au bitfield directement.

La méthode retourne le résultat de validation, sans produire d’`Output` :

- aucune écriture disque ;
- aucune annonce `Have` pendant la restauration ;
- aucun événement de complétion réseau.

L’erreur de pièce est exposée sous le nom `PieceError`, afin que l’orchestrateur puisse la propager sans dépendre du module interne `piece_manager`.

Quand les peers se connectent ensuite, le chemin habituel `on_connected` annonce le bitfield restauré, ou `HaveAll` si le torrent est complet et l’extension Fast négociée.

## 5. SwarmIO : orchestrer la vérification initiale

**Fichier :** `tortue_lib/src/adapters/swarm_io.rs`

`SwarmIO` possède désormais le `Swarm` construit dans `new`. Auparavant, `run` créait un swarm local ; cela aurait perdu toute restauration effectuée avant son lancement.

`restore()` parcourt les hashes du metainfo :

```text
offset = index × piece_length
longueur = min(piece_length, total_size - offset)
```

Les calculs d’offset et de taille restante sont contrôlés.

Pour chaque pièce :

- lecture via `PieceStore` ;
- si la lecture est complète, appel de `Swarm::restore_piece` ;
- si elle est courte, aucune restauration ;
- si le hash est incorrect, aucune restauration ;
- si une erreur IO ou de cohérence survient, propagation à l’appelant.

Les pièces sont lues une par une. Aucun buffer intermédiaire contenant tout le torrent n’est créé.

À la fin, `restore()` publie un snapshot. `run()` utilise ensuite ce même swarm ; il ne le recrée pas et ne refait pas la vérification.

**Contrat de démarrage :** `start_download` appelle explicitement `restore()` avant `run()`. Un autre appelant de `SwarmIO` doit respecter cet ordre pour bénéficier de la reprise.

## 6. Statistiques : contenu disponible et trafic réseau

**Fichiers :**

- `tortue_lib/src/domain/swarm/piece_manager.rs`
- `tortue_lib/src/domain/swarm.rs`
- `tortue_lib/src/adapters/swarm_io.rs`

Le snapshot distingue maintenant :

| Champ              | Sens                                                                 |
| ------------------ | -------------------------------------------------------------------- |
| `bytes_total`      | Taille totale du contenu                                             |
| `bytes_available`  | Taille des pièces complètes et validées, y compris celles restaurées |
| `bytes_downloaded` | Compteur de données reçues par le chemin de téléchargement           |
| `bytes_uploaded`   | Compteur existant d’upload                                           |

`available_bytes()` somme la taille des pièces complètes. Le calcul du tracker devient :

```rust
left = bytes_total.saturating_sub(bytes_available);
```

Il ne dépend plus du trafic téléchargé, qui peut inclure des données invalides ou redondantes.

### Exemple de reprise

```text
Torrent : 100 MiB
Pièces locales validées : 60 MiB

bytes_available  = 60 MiB
bytes_downloaded = 0
left             = 40 MiB
```

Les vitesses restent calculées à partir des compteurs réseau : la reprise ne provoque pas un faux pic de débit.

Un consommateur de l’API qui veut afficher la quantité de contenu validé doit utiliser `bytes_available`, pas `bytes_downloaded`.

## 7. Ordre des trackers et propagation des erreurs

**Fichier :** `tortue_lib/src/application/download.rs`

`start_download` appelle et attend `coordinator.restore()` avant de lancer les tâches tracker et la boucle du swarm.

Le snapshot restauré met à jour les `SessionStats` partagées. Les trackers lisent donc un `left` fondé sur les pièces validées dès leur première annonce.

Si la restauration échoue, le démarrage retourne une erreur applicative avant de lancer les trackers. Dans le modèle actuel, les tâches/fichiers de stockage ont déjà été créés ; ce n’est pas une opération transactionnelle sur le système de fichiers.

## Vérifications effectuées

- Les 32 tests existants passent.
- Un programme de vérification exécuté hors du dépôt a contrôlé :
  - la lecture d’une pièce traversant deux fichiers ;
  - le rejet d’une pièce corrompue ;
  - la restauration de la dernière pièce plus courte ;
  - l’absence d’incrément du compteur réseau lors de la restauration ;
  - la conservation du contenu existant pendant la lecture ;
  - le rejet d’un débordement de plage de lecture.

Aucun test supplémentaire n’a été ajouté au dépôt.

## Limites conservées

- **Mémoire :** les pièces restaurées restent en RAM, comme les pièces téléchargées. Le buffer de lecture est borné à une pièce, mais le stockage en mémoire du contenu validé reste proportionnel au torrent.
- **Durée de démarrage :** la vérification est complète à chaque démarrage ; il n’existe pas encore de fast-resume persistant ni d’annulation dédiée à cette phase.
- **Erreurs d’écriture :** les lectures retournent leurs erreurs, mais les écritures du worker conservent leur traitement précédent par log. La garantie de complétion disque reste un chantier distinct.
- **File de stockage :** le canal reste non borné.
- **Sécurité des chemins :** la restauration ne résout pas la protection contre les symlinks, les collisions de fichiers ou l’autorisation de réparer des données existantes.
- **Métadonnées :** les contrôles ajoutés aux offsets ne remplacent pas la validation complète des invariants du metainfo.
- **Torrent déjà complet :** la restauration établit sa disponibilité et `left = 0`, mais n’émet pas un `Output::Completed`. La notification publique de ce cas reste une décision séparée.
