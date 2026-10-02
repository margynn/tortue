# Revue d’architecture — points à corriger

## Bilan

Le design est globalement bon : le domaine décide via `Swarm::step(Input) -> Vec<Output>`, les adapters exécutent les effets, et la boucle `SwarmIO` reste le seul propriétaire de l’état du swarm.

Les problèmes prioritaires concernent les garanties entre composants : stockage, sécurité des entrées, envoi des commandes et arrêt des tâches. Il n’est pas nécessaire de réécrire l’architecture ou de multiplier les abstractions.

Cette revue porte sur le cœur du swarm, le stockage, les connexions, les trackers et l’orchestration. Ce n’est pas un audit exhaustif des codecs. Au moment de la revue, les 32 tests de `cargo test -p tortue_lib` passent.

## 1. Sécuriser les chemins et empêcher les écrasements involontaires

**Priorité : haute — sécurité et perte de données.**

**Fichiers :**

- `tortue_lib/src/domain/torrent.rs`
- `tortue_lib/src/adapters/disk_storage.rs`

### Constat

`Metainfo.name` et les composants de `File.path` proviennent du torrent et sont utilisés directement pour construire les chemins de sortie. Les chemins absolus et les composants `..` peuvent permettre de sortir du dossier de téléchargement.

Les fichiers sont ouverts avec `truncate(true)` : un chemin malveillant peut provoquer un écrasement extérieur, et relancer un téléchargement détruit les données déjà présentes.

### À faire

- [ ] Valider les noms et composants de chemins issus du torrent ; refuser notamment les chemins absolus, remontées et séparateurs incorporés permettant une sortie de destination.
- [ ] Vérifier les règles sur les plateformes supportées.
- [ ] Définir la politique concernant les symlinks déjà présents : une validation lexicale ne suffit pas à garantir le confinement.
- [ ] Tant que la reprise n’est pas implémentée, refuser d’écraser les fichiers existants, ou exiger une décision explicite de l’appelant.

### Vérification minimale

Un torrent avec un chemin dangereux est refusé sans création ni modification de fichier extérieur. Un fichier existant n’est pas tronqué silencieusement.

## 2. Ne pas annoncer la réussite avant la fin des écritures

**Priorité : haute — intégrité des données.**

**Fichiers :**

- `tortue_lib/src/domain/swarm.rs`
- `tortue_lib/src/domain/swarm/piece_manager.rs`
- `tortue_lib/src/adapters/swarm_io.rs`
- `tortue_lib/src/adapters/disk_storage.rs`
- `tortue_lib/src/application/ports/piece_store.rs`

### Constat

Le domaine considère une pièce acquise après validation du hash et produit `Have`, `WritePiece`, puis éventuellement `Completed`.

Mais `DiskStorage::write()` confirme seulement la mise en file. La tâche disque écrit plus tard et logue les erreurs sans les remonter. Le téléchargement peut donc être considéré terminé alors que les écritures échouent ou restent en attente.

### À faire

- [ ] Clarifier le contrat de `PieceStore` : mise en file, écriture terminée et persistance après crash sont des garanties différentes.
- [ ] Remonter les erreurs de la tâche disque à l’orchestrateur et au résultat public du téléchargement.
- [ ] Ne pas annoncer la réussite publique avant la fin effective des écritures attendues.
- [ ] Prévoir un chemin de drainage/fermeture du stockage lors de l’arrêt.
- [ ] Définir séparément si une garantie de persistance après crash est nécessaire ; ne pas la confondre avec la fin d’une écriture.

Le correctif initial peut rester simple : propagation des erreurs et attente de la fin des écritures. Un événement domaine par écriture n’est pas nécessaire par principe.

### Vérification minimale

Une erreur disque empêche l’annonce de réussite et devient une erreur observable par l’appelant. Une file contenant encore des écritures ne permet pas une réussite prématurée.

## 3. Traiter les commandes réseau refusées

**Priorité : haute — cohérence domaine/transport et progression.**

**Fichiers :**

- `tortue_lib/src/adapters/swarm_io.rs`
- `tortue_lib/src/adapters/metadata_io.rs`
- `tortue_lib/src/domain/swarm.rs`
- `tortue_lib/src/domain/swarm/block_assignment.rs`

### Constat

Les erreurs de `try_send()` sont ignorées. Or le domaine a parfois déjà enregistré l’effet attendu : `send_request()` marque un bloc comme demandé avant que la commande soit acceptée par le canal.

Si le canal est plein, la requête est perdue mais reste enregistrée comme en vol. Les assignments n’expirant pas actuellement, cela peut bloquer la progression.

### À faire

- [ ] Traiter explicitement les erreurs `Full` et `Closed` pour les envois directs et broadcasts.
- [ ] Choisir une politique qui maintient la cohérence : par exemple, déconnecter le peer dont la file n’accepte plus les commandes et libérer ses assignments.
- [ ] Faire passer ce nettoyage par le chemin de déconnexion du domaine, pas seulement supprimer un sender.
- [ ] Éviter d’attendre sans limite sur un peer lent dans la boucle centrale du swarm.
- [ ] Examiner la récupération des requêtes restées sans réponse : un peer qui envoie des keepalives peut rester connecté sans jamais livrer les blocs demandés.

### Vérification minimale

Une file de commandes saturée ne laisse pas un bloc définitivement enregistré comme demandé sans possibilité de reprise.

## 4. Fiabiliser le cycle de vie des tâches

**Priorité : haute — arrêt et nettoyage.**

**Fichiers :**

- `tortue_lib/src/application/download.rs`
- `tortue_lib/src/application/magnet.rs`
- `tortue_lib/src/adapters/swarm_io.rs`
- `tortue_lib/src/adapters/metadata_io.rs`
- `tortue_lib/src/adapters/peer_io.rs`
- `tortue_lib/src/adapters/tracker_io.rs`
- `tortue_lib/src/adapters/disk_storage.rs`

### Constat

Plusieurs tâches sont lancées sans conserver leur handle ni observer leur résultat. L’abandon d’un `JoinHandle` ne termine pas la tâche.

Cas relevés :

- `SwarmHandle::shutdown()` passe le domaine à `Stopped`, mais ne termine pas `SwarmIO::run()`.
- Une erreur définitive de `connect_with_retry(...).await?` saute le sentinel `Disconnected`.
- Une erreur d’écriture dans `run_session` peut laisser la tâche reader active.
- Le tracker UDP attend dans `recv()` sans timeout.
- Un tracker en échec permanent peut continuer après fermeture du swarm, car il ne constate la fermeture du canal qu’après une annonce réussie.

### À faire

- [ ] Distinguer pause/statut arrêté et arrêt effectif de la tâche ; clarifier le contrat public de `shutdown()`.
- [ ] Assurer le nettoyage sur toutes les sorties : succès, erreur, annulation et fermeture des canaux.
- [ ] Arrêter le reader à chaque fin de session.
- [ ] Émettre `Disconnected` après une fin définitive du runner sortant, y compris après échec de connexion ; ne pas l’émettre avant une reconnexion interne.
- [ ] Vérifier les annulations déjà présentes, pas uniquement attendre un nouveau `watch::Receiver::changed()`.
- [ ] Donner aux trackers un chemin d’annulation indépendant du succès réseau.
- [ ] Ajouter un timeout aux échanges UDP.
- [ ] Conserver/observer les handles nécessaires pour arrêter et attendre les tâches possédées par un téléchargement.
- [ ] Nettoyer aussi les tâches temporaires utilisées pour récupérer les métadonnées d’un magnet.
- [ ] Inclure le drainage ou l’échec du stockage dans la procédure d’arrêt.

Pas besoin d’un framework de supervision : une propriété explicite des tâches et un mécanisme d’annulation suffisent.

### Vérification minimale

Fermer ou arrêter un téléchargement termine ses tâches dans un délai borné, sans reader restant. Un échec définitif de connexion produit bien la déconnexion attendue.

## 5. Borner l’utilisation mémoire

**Priorité : moyenne à haute — capacité à télécharger de gros torrents.**

**Fichiers :**

- `tortue_lib/src/domain/swarm/piece_manager.rs`
- `tortue_lib/src/adapters/disk_storage.rs`
- `tortue_lib/src/application/ports/piece_store.rs`

### Constat

Les blocs reçus restent en mémoire même après validation et écriture afin de servir les uploads. Le téléchargement peut donc retenir approximativement la taille entière du torrent en RAM.

S’ajoutent le buffer assemblé d’une pièce, la copie faite par `DiskStorage::write()` et une file d’écriture non bornée.

### À faire

- [ ] Borner la file d’écriture et définir la réaction lorsque le disque ne suit pas.
- [ ] Garder les pièces en cours en mémoire, puis libérer les données devenues disponibles sur disque.
- [ ] Faire évoluer le stockage pour lire les blocs destinés aux uploads.
- [ ] Ne libérer les données qu’après confirmation de l’écriture réussie.
- [ ] Limiter les copies inutiles aux frontières de stockage lorsque le contrat sera clarifié.

### Vérification minimale

L’utilisation mémoire ne croît plus avec toutes les pièces déjà téléchargées ; un disque lent ne provoque pas une croissance illimitée de la file.

## 6. Valider les invariants des données externes

**Priorité : haute pour les entrées non fiables.**

**Fichiers :**

- `tortue_lib/src/domain/torrent.rs`
- `tortue_lib/src/domain/metadata.rs`
- `tortue_lib/src/domain/swarm/piece_manager.rs`

### Constat

Le parsing contrôle certains types et signes, mais pas tous les invariants utilisés ensuite par le domaine :

- `piece_length` peut être nul ;
- le nombre de hashes n’est pas vérifié contre la taille totale ;
- les sommes et calculs de tailles ne sont pas tous contrôlés ;
- un `metadata_size` annoncé par un peer peut déclencher une allocation sans plafond raisonnable.

### À faire

- [ ] Refuser une longueur de pièce nulle.
- [ ] Vérifier la cohérence entre taille totale, longueur de pièce et nombre de hashes.
- [ ] Utiliser des calculs contrôlés pour les tailles, offsets, sommes et conversions nécessaires.
- [ ] Fixer un plafond explicite pour les métadonnées annoncées par un peer.
- [ ] Vérifier la taille et la cohérence des fragments de métadonnées reçus.
- [ ] Établir les invariants à la construction des objets, avant qu’ils alimentent les calculs du swarm.

### Vérification minimale

Des métadonnées incohérentes ou excessives sont refusées proprement, sans panic ni allocation démesurée.

## 7. Corriger le contrat de récupération des magnets

**Priorité : haute — fonctionnalité incompatible entre composants.**

**Fichiers :**

- `tortue_lib/src/application/magnet.rs`
- `tortue_lib/src/domain/metadata.rs`
- `tortue_lib/src/domain/torrent.rs`

### Constat

`Metadata` retourne les octets validés du dictionnaire `info`. `fetch_metadata()` les transmet à `Metainfo::try_from`, qui attend un torrent complet contenant notamment `announce` et `info`.

### À faire

- [ ] Fournir un chemin de construction de `Metainfo` à partir des octets `info` validés et des trackers du magnet.
- [ ] Réutiliser le parsing existant du dictionnaire `info` plutôt que dupliquer ses règles.
- [ ] Préserver la vérification de l’info hash attendu.

### Vérification minimale

Des octets `info` valides récupérés pour un magnet produisent un `Metainfo` utilisable, avec les trackers du magnet, sans exiger une enveloppe de fichier `.torrent`.

## 8. Nettoyer la sémantique des événements tracker

**Priorité : moyenne — protocole et cycle de vie.**

**Fichier :** `tortue_lib/src/adapters/tracker_io.rs`

### Constat

L’événement démarre à `Started` et n’est pas remis à une annonce normale après succès. `Completed` peut aussi être répété sur chaque annonce dès que `left == 0`. Le choix de `Stopped` dépend de la possibilité d’uploader plutôt que d’un arrêt effectif de la session.

### À faire

- [ ] Modéliser une annonce normale sans événement.
- [ ] Envoyer `Started` et `Completed` selon les transitions appropriées, plutôt qu’en continu.
- [ ] Relier `Stopped` à l’arrêt effectif de la participation au torrent.
- [ ] Définir la sémantique d’une pause et du mode download-only vis-à-vis du tracker.

### Vérification minimale

Une séquence démarrage → annonces normales → complétion → arrêt produit les événements attendus sans répétition systématique.

## 9. Garder le découpage pragmatique des couches

**Priorité : basse — pas une réécriture nécessaire.**

### Constat

`application/download.rs` assemble directement les adapters concrets. L’application joue donc aussi le rôle de point de composition ; ce n’est pas une architecture hexagonale strictement orientée vers l’intérieur.

`SwarmIO` est principalement un orchestrateur utilisant des ports : il pourrait vivre dans l’application. Son emplacement actuel dans les adapters n’est pas en soi un défaut fonctionnel.

### Recommandations

- [ ] Garder les règles métier dans `Swarm`, indépendantes de Tokio et des sockets.
- [ ] Garder le parsing/handshake TCP et les opérations disque dans les adapters.
- [ ] Éviter d’ajouter des abstractions uniquement pour satisfaire un diagramme.
- [ ] Extraire un module de composition `client`/`bootstrap` seulement si plusieurs configurations ou le démarrage global du listener le justifient.

### Registres

- `PeerRegistry` : état des pairs d’un torrent, dans le domaine ; la détection des doublons par `PeerId` y a sa place.
- `SwarmRegistry` : routage global `InfoHash → Sender<InboundPeer>` ; avec `InboundPeer` contenant un `TcpStream`, sa place dans les adapters est cohérente.
- Le connector sortant n’a pas besoin du registre global : il connaît déjà son swarm.

La détection des doublons et le branchement entrant restent des travaux du plan `plan_peer_listener.md`, pas une raison de refondre les couches.

## 10. Ajouter des tests sur les contrats essentiels

**Priorité : accompagner chaque correction.**

Les tests existants portent principalement sur les structures et codecs de base. La forme `step(Input) -> Vec<Output>` permet de tester le swarm sans réseau réel.

- [ ] Échec disque : aucune réussite publique annoncée.
- [ ] Fermeture/annulation d’un peer : nettoyage, sans reconnexion volontaire ni reader restant.
- [ ] File de commandes saturée : assignments libérés ou autre récupération explicite.
- [ ] Chemin de torrent dangereux : refus sans écriture extérieure.
- [ ] Pièce invalide puis valide : reprise et une seule complétion.
- [ ] Métadonnées magnet : construction correcte depuis les octets `info`.
- [ ] Tailles et métadonnées incohérentes : erreur contrôlée.
- [ ] Arrêt du téléchargement : fin des tâches et résultat du stockage observé.

Privilégier un petit test reproductible par garantie corrigée, sans imposer de nouveau framework.

## Ordre de travail recommandé

1. **Chemins et écrasements** : empêcher les dégâts sur les fichiers.
2. **Invariants des entrées et contrat magnet** : refuser les données incohérentes et rétablir la récupération des métadonnées.
3. **Erreurs et fin des écritures disque** : rendre la réussite fiable.
4. **Arrêt et nettoyage des tâches** : définir qui possède et termine chaque tâche.
5. **Envois réseau refusés et récupération des requêtes** : empêcher les blocages silencieux.
6. **Mémoire bornée et uploads depuis le disque** : supporter les gros torrents.
7. **Événements tracker** : aligner les annonces sur les transitions réelles.
8. **Placement des modules** : seulement si un besoin concret apparaît.

Les tests doivent accompagner ces étapes, pas être reportés à la fin. Aucun changement de code n’est effectué par ce document.
