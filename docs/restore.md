# Reprise d’un téléchargement : restauration des pièces locales

## Objectif

**Évolution prévue :** les sections 3 à 5 décrivent désormais la restauration en réutilisant `PieceManager::receive_block`, sans ajouter `PieceManager::restore_piece`. Cette évolution est un plan à implémenter, pas une description d’un changement déjà effectué dans le code.

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
    │           → lecture des blocs présents, même si la pièce est incomplète
    │           → entrée locale dédiée du Swarm pour chaque bloc entier
    │           → PieceManager::receive_block
    │           → validation SHA-1 et mise à jour du bitfield
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

## 3. PieceManager : réutiliser `receive_block`

**Fichier :** `tortue_lib/src/domain/swarm/piece_manager.rs`

Ne pas ajouter de méthode `restore_piece` dans `PieceManager`. Le mécanisme existant `receive_block` sait déjà :

1. Vérifier l’index et la taille du bloc.
2. Stocker les octets et mettre à jour l’état des blocs.
3. Assembler une pièce lorsque tous ses blocs sont présents.
4. Vérifier son SHA-1.
5. Réinitialiser les blocs si le hash est incorrect.
6. Positionner le bit de disponibilité si le hash est valide.

### Retirer le comptage réseau de cette méthode

Déplacer l’incrément de `downloaded_bytes` dans l’appelant réseau du `Swarm`, à l’emplacement correspondant au comportement actuel. La restauration locale ne doit jamais incrémenter ce compteur. Conserver les compteurs par peer existants et éviter de compter deux fois le même événement.

`receive_block` devient ainsi indépendant de l’origine des octets : disque ou réseau. Ne pas ajouter de booléen `is_restore` ou `count_download` à sa signature.

### Résultats pendant la restauration

| Résultat                                           | Action                                                                        |
| -------------------------------------------------- | ----------------------------------------------------------------------------- |
| `Ok(None)` sur un bloc intermédiaire               | Continuer à fournir les blocs de cette pièce                                  |
| `Ok(None)` sur le dernier bloc avec hash incorrect | La méthode a réinitialisé la pièce ; elle reste manquante                     |
| `Ok(Some(CompletedPiece))`                         | Pièce validée et bitfield mis à jour ; ignorer le résultat pour les effets IO |
| `Err(...)`                                         | Erreur de cohérence, à traiter explicitement                                  |

La restauration travaille sur un manager neuf, avant les échanges réseau. Fournir les blocs entiers disponibles, même si la pièce est incomplète. Un bloc fait normalement 16 KiB ; le dernier bloc d’une pièce peut avoir une longueur attendue plus courte.

- Pièce incomplète : conserver les blocs locaux reçus et demander uniquement ceux qui manquent.
- Pièce complète et SHA-1 valide : déclarer la pièce disponible.
- Pièce complète et SHA-1 invalide : réinitialiser toute la pièce et la retélécharger. Le hash ne permet pas d’identifier le bloc corrompu individuellement.
- Bloc tronqué : ne pas l’ingérer ; il reste manquant.

Un bloc reçu n’est pas nécessairement validé. **Ne jamais annoncer ni servir les données d’une pièce avant validation de son hash.** Ajouter une garde dans `PieceManager::read_block` sur la disponibilité validée : le code actuel peut lire les blocs reçus d’une pièce incomplète.

### Pourquoi ne pas seulement charger le bitfield ?

Les uploads lisent actuellement les blocs depuis `PieceManager`, en mémoire. Il faut restaurer les octets, pas seulement déclarer la disponibilité.

## 4. Swarm : distinguer l’origine des données, pas leur validation

**Fichier :** `tortue_lib/src/domain/swarm.rs`

Garder `PieceManager` privé. Prévoir une entrée locale dédiée au démarrage, par exemple `Input::LocalBlock { piece_index, piece_offset, data }`, traitée par un handler privé du `Swarm`. Elle transporte un bloc entièrement lu, sans exposer les champs du manager à `SwarmIO`.

Le handler local :

1. Vérifie l’index, l’alignement de l’offset et la longueur attendue du bloc avant toute mutation.
2. Construit le `BlockRef` et appelle `self.pieces.receive_block(block_ref, data)`.
3. Ignore `CompletedPiece` pour les effets IO : le bitfield a déjà été mis à jour.

Réutiliser la taille et le découpage des blocs existants ; ne pas recopier une constante de 16 KiB dans l’orchestrateur.

Le chemin local ne produit ni `WritePiece`, ni `Have`, ni `Completed`, et ne modifie pas les compteurs réseau. Le traitement d’une erreur de cohérence doit être explicite : le contrat actuel `step -> Vec<Output>` ne permet pas de la propager avec `?`. Définir ce traitement lors du branchement de l’entrée locale, sans transformer une erreur IO disque en simple mismatch de hash.

Le handler réseau `on_message_piece` reste distinct : il vérifie les assignments, comptabilise le trafic et produit les effets nécessaires. **Ne pas l’appeler pour simuler une réception depuis un faux peer.**

Les deux handlers réutilisent le même `PieceManager::receive_block` pour le stockage et la validation. Aucun second algorithme de validation n’est nécessaire.

Quand les peers se connectent ensuite, `on_connected` annonce le bitfield restauré, ou `HaveAll` si le torrent est complet et l’extension Fast négociée.

## 5. SwarmIO : orchestrer la vérification initiale

**Fichier :** `tortue_lib/src/adapters/swarm_io.rs`

`SwarmIO` possède désormais le `Swarm` construit dans `new`. Auparavant, `run` créait un swarm local ; cela aurait perdu toute restauration effectuée avant son lancement.

`restore()` parcourt les hashes du metainfo :

```text
offset = index × piece_length
longueur = min(piece_length, total_size - offset)
```

Les calculs d’offset et de taille restante sont contrôlés.

Pour chaque pièce, parcourir ses plages de blocs attendues et lire chaque bloc indépendamment via `PieceStore` :

- bloc entièrement présent sur disque : transmettre l’entrée locale et appeler `receive_block` ;
- lecture courte ou plage absente : ne transmettre aucun octet de ce bloc ; continuer avec les autres plages ;
- hash incorrect lorsque la pièce devient complète : `receive_block` réinitialise toute la pièce ;
- erreur IO réelle ou erreur de cohérence : traitement explicite, sans la confondre avec des données absentes.

Un bloc peut traverser plusieurs fichiers : toute sa plage doit être présente. Ne pas concaténer des données après un trou pour remplir artificiellement le bloc ; conserver les offsets logiques du torrent.

**Présence des données et `set_len` :** conserver les tailles originales des fichiers (un fichier créé au démarrage n’avait aucune donnée locale), ou effectuer la lecture avant leur extension. Les zéros ajoutés par le dimensionnement ne doivent pas faire passer une plage absente pour un bloc reçu. Ce suivi concerne la lecture de reprise, pas les lectures normales après téléchargement.

Les blocs sont lus successivement ; aucun buffer intermédiaire contenant tout le torrent n’est créé. Des blocs locaux non vérifiés peuvent rester en mémoire jusqu’à la réception des blocs manquants.

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

Ces vérifications concernent l’implémentation précédente. Après l’évolution vers `receive_block`, réexécuter les contrôles ci-dessous, notamment le comptage réseau, la conservation des blocs entiers d’une pièce incomplète, le refus d’upload avant validation et la distinction entre données présentes et zéros ajoutés par `set_len`.

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
