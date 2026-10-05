# Revue d’architecture — état courant et points à corriger

## 2. Écritures disque et complétion

**Priorité : haute — intégrité. Statut : barrière de complétion implémentée, fermeture incomplète.**

**Fichiers :** `application/ports/piece_store.rs`, `adapters/disk_storage.rs`, `adapters/swarm_io.rs`, `domain/swarm.rs`, `src/main.rs`.

### Contrat actuel

`PieceStore::write()` transfère un `Vec<u8>` et attend sa mise dans une file bornée. Ce n’est toujours pas un acquittement d’écriture. Le worker traite les commandes séquentiellement ; `flush()` attend les commandes précédentes et le `flush()` Tokio de chaque fichier, **pas** une persistance après crash (`sync_all`).

Le domaine produit toujours `Have`, `WritePiece`, puis éventuellement `Completed`. `SwarmIO` traite ces sorties dans l’ordre et attend `flush()` sur `Completed` **avant de publier le snapshot complet**. Une erreur de write/flush remonte par `SwarmIO::run()` puis `Download.task`. La CLI surveille cette tâche et utilise les octets vérifiés disponibles, pas seulement le nombre de blocs reçus, pour afficher la réussite.

Si le worker échoue pendant une écriture, il logue l’erreur et ferme le canal. Un write/read/flush ultérieur constate cette fermeture, mais l’appelant reçoit généralement une erreur générique de canal plutôt que la cause disque initiale. Sans nouvelle opération de stockage, l’erreur n’est pas immédiatement observée.

### Restant à faire

- [ ] Remonter la cause initiale et permettre l’observation d’un échec du worker même sans nouvelle commande disque.
- [ ] Drainer/fermer le stockage sur les sorties autres que `Completed` ; `run_loop()` peut retourner `Ok(())` après fermeture des commandes sans barrière disque.
- [ ] Préciser la garantie publique : `Download.task` représente la vie du coordinateur, pas un futur résolu à la complétion ; le swarm peut continuer à servir après le téléchargement.
- [ ] Documenter que `Have` et l’état interne « pièce complète » précèdent encore l’écriture effective. La publication de complétion est protégée, pas chaque annonce réseau de disponibilité.

**Vérification minimale :** une écriture échouée ou encore en attente empêche le snapshot de complétion et la réussite CLI ; un arrêt observe également le résultat du stockage.

## 3. Commandes réseau et requêtes sans réponse

**Priorité : haute — progression. Statut : corrigé côté swarm, pas côté métadonnées.**

**Fichiers :** `adapters/swarm_io.rs`, `adapters/metadata_io.rs`, `domain/swarm.rs`, `domain/swarm/block_assignment.rs`.

### Ce qui a changé

`SwarmIO::send_to_peer()` attend `send()` avec un timeout de **20 secondes**. Canal absent, fermé ou timeout déclenchent `DisconnectPeer` : suppression du sender, annulation du connector, puis `Input::PeerDisconnected` dans le domaine. Ce chemin libère les assignments et annule les uploads en attente ; il est aussi utilisé pour les broadcasts.

### Restant à faire

- [ ] Traiter les `try_send()` encore ignorés dans `MetadataIO`, avec nettoyage du domaine de métadonnées. Le batch de requêtes de métadonnées peut dépasser la file de 256 commandes.
- [ ] Borner le blocage **global** du coordinateur : attendre jusqu’à 20 s par envoi est fini, mais un broadcast vers plusieurs peers lents cumule ces attentes et retarde commandes, ticks et événements.
- [ ] Récupérer les requêtes sans réponse. `BlockAssignments` stocke des instants mais n’expire pas les assignments ; un peer qui continue les keepalives peut occuper ses 32 slots indéfiniment.

Le scheduler réplique les blocs vers d’autres peers lorsqu’il reste du budget, mais cela ne garantit pas la progression s’il n’existe pas d’autre peer disponible.

**Vérification minimale :** une file saturée libère les assignments ; un peer connecté mais ne livrant jamais ses blocs ne peut bloquer définitivement le téléchargement.

## 4. Cycle de vie et arrêt

**Priorité : haute — nettoyage. Statut : sessions TCP améliorées, propriété globale des tâches manquante.**

**Fichiers :** `application/download.rs`, `application/magnet.rs`, `adapters/swarm_io.rs`, `adapters/metadata_io.rs`, `adapters/peer_io.rs`, `adapters/tracker_io.rs`, `adapters/disk_storage.rs`.

### Ce qui a changé

- `run_session()` sélectionne directement entre lecture et écriture : la branche perdante est abandonnée avec la session, sans reader détaché.
- La session et les tentatives sortantes vérifient l’annulation déjà présente ou la fermeture du canal watch.
- Les wrappers du connector envoient `Disconnected` après la fin définitive du runner, y compris un échec de connexion. Une reconnexion interne n’émet pas ce sentinel prématurément.
- Les lectures d’upload sont possédées par un `JoinSet`, annulables par identifiant et annulées/rejointes après la sortie de `run_loop()`.

### Restant à faire

- [ ] Donner un arrêt effectif à `SwarmHandle::shutdown()` : aujourd’hui il change seulement le statut en `Stopped`, sans quitter la boucle, déconnecter tous les peers ni fermer le stockage.
- [ ] Conserver et observer les tâches peer/tracker/disque nécessaires à un arrêt attendu ; leurs `JoinHandle` sont encore abandonnés.
- [ ] Donner aux trackers une annulation indépendante du réseau : la fermeture de leur sender n’est constatée qu’après une annonce réussie. Les trackers sont même lancés avant l’ouverture du stockage.
- [ ] Ajouter un timeout aux deux `recv()` UDP ; HTTP a déjà un timeout de 10 s.
- [ ] Nettoyer les tâches temporaires de récupération magnet sur succès, erreur et annulation ; borner également la durée totale de récupération.
- [ ] Nettoyer les senders d’annulation des peers terminés naturellement, encore conservés dans `peer_cancels` jusqu’à déconnexion explicite ou destruction du connector.
- [ ] Relier la fin CLI à une fermeture contrôlée : elle sort dès le snapshot complet, sans arrêter ni attendre toutes les tâches du téléchargement.

**Vérification minimale :** shutdown, fermeture et erreur terminent les tâches possédées dans un délai borné, avec stockage observé et aucun runner restant.

## 5. Mémoire et coût du scheduler

**Priorité : moyenne à haute — gros torrents. Statut : rétention des pièces complètes corrigée, budget global non garanti.**

**Fichiers :** `domain/swarm/piece_manager.rs`, `domain/swarm.rs`, `adapters/disk_storage.rs`, `adapters/swarm_io.rs`.

### Ce qui a changé

- Une pièce validée devient `PieceState::Complete` sans conserver ses blocs ; son buffer est transféré au stockage sans copie à cette frontière.
- La file disque est bornée à **8 commandes**, pas à un nombre d’octets.
- Les uploads utilisent `ReadForUpload`, partagent une lecture pour une même plage et gèrent `Cancel`/déconnexion/désactivation de l’upload. Le domaine limite les demandes en attente à **32** associations peer/plage.
- Le scheduler privilégie les pièces partielles avant les pièces suggérées et rares, et calcule les nombres de holders une fois par planification.

### Restant à faire

- [ ] Mesurer puis borner le nombre de pièces partielles et de peers si nécessaire : la priorité aux pièces partielles réduit la dispersion, sans constituer un plafond mémoire.
- [ ] Tenir compte de la taille des pièces dans le budget disque ; huit pièces très grandes restent coûteuses. L’assemblage conserve temporairement les blocs et le buffer contigu.
- [ ] Réduire le coût de `plan()` sur les gros torrents : il trie les pièces nécessaires et reconstruit la liste de tous les blocs manquants à chaque appel disposant de budget, puis peut rescanner par profondeur de réplication.

`docs/plan_scaling.md` reste une proposition de scheduler à deux modes, **non implémentée** ; ce n’est pas une description du comportement actuel. Une queue incrémentale/cache ne se justifie qu’après mesure et avec des règles de mise à jour explicites.

**Vérification minimale :** la RAM ne retient plus toutes les pièces complètes ; tester un disque lent et mesurer le temps de planification sur un gros torrent.

## 6. Invariants des entrées externes

**Priorité : haute — données non fiables et torrents valides refusés. Statut : validation ajoutée mais incorrecte/incomplète.**

**Fichiers :** `domain/torrent.rs`, `domain/metadata.rs`, `domain/swarm/piece_manager.rs`, `adapters/disk_storage.rs`.

`Metainfo::validate()` impose un plafond de **40 Gio** et compare `piece_length * pieces.len()` à la taille totale. Mais il refuse lorsque le maximum est **inférieur ou égal** à la taille réelle : un torrent dont la dernière pièce est pleine est donc refusé alors qu’il est valide. Inversement, un excès de hashes passe ce contrôle et peut provoquer une soustraction invalide lors de la construction de la dernière pièce.

Les sommes et produits ne sont pas tous contrôlés. Les champs de `Metainfo` restent publics : les invariants du parser ne sont pas garantis pour une construction directe.

Le stockage vérifie maintenant les indices, les additions/multiplications d’offsets, la taille des données et les bornes du torrent. Les demandes d’upload sont également bornées à une plage valide d’au plus un bloc. Ces protections ne remplacent pas la validation du metainfo.

### Restant à faire

- [ ] Vérifier explicitement `piece_length > 0` et `pieces.len() == ceil(total_size / piece_length)`, avec une politique explicite pour le torrent vide.
- [ ] Contrôler les sommes, produits et conversions avant de construire les états du swarm ; appliquer les invariants aux points de construction utilisables.
- [ ] Plafonner `metadata_size` avant `vec![None; count]` dans `Metadata`.
- [ ] Vérifier les tailles des fragments reçus, leur `total_size`, la cohérence entre peers et la taille finale avant concaténation ; le hash final ne suffit pas à borner les ressources consommées.

**Vérification minimale :** torrent à dernière pièce pleine accepté ; hashes en trop/en moins, tailles nulles, débordements et métadonnées excessives refusés sans panic ni allocation démesurée.

## 7. Contrat magnet

**Priorité : haute — fonctionnalité toujours incompatible. Statut : non corrigé.**

**Fichiers :** `application/magnet.rs`, `domain/metadata.rs`, `domain/torrent.rs`.

`Metadata` retourne toujours les octets du dictionnaire `info` validés par SHA-1. `fetch_metadata()` les passe encore à `Metainfo::try_from()`, qui attend une enveloppe contenant `announce` et `info`. Les trackers du magnet ne sont pas réinjectés dans un metainfo construit depuis ces octets.

- [ ] Construire `Metainfo` depuis les octets `info` validés et les trackers du magnet, en réutilisant `parse_info()` et la validation commune.
- [ ] Préserver le hash attendu et les octets d’info correspondants.
- [ ] Ajouter le test de ce contrat indépendamment d’un tracker ou d’un peer réel.

**Vérification minimale :** les octets `info` récupérés produisent un metainfo utilisable sans enveloppe `.torrent`.

## 8. Événements tracker

**Priorité : moyenne — protocole. Statut : non corrigé.**

**Fichiers :** `adapters/tracker_io.rs`, `domain/tracker.rs`.

L’événement reste `Started` après succès, puis peut rester `Completed` à toutes les annonces lorsque `left == 0`. `Stopped` dépend encore de l’absence d’upload, pas de la fin de participation ; la condition `left == 0` peut ensuite le remplacer par `Completed`. Il n’existe pas d’annonce normale sans événement.

- [ ] Envoyer `Started`/`Completed` selon les transitions, puis une annonce sans événement.
- [ ] Relier `Stopped` à l’arrêt réel et définir pause/download-only.
- [ ] Tester la séquence démarrage → annonces normales → complétion → arrêt, y compris un torrent repris déjà complet.

## 9. Connexions entrantes et découpage des couches

**Priorité : moyenne pour l’intégration entrante, basse pour le placement des modules. Statut : briques présentes, chemin public non branché.**

**Fichiers :** `adapters/peer_io.rs`, `adapters/swarm_registry.rs`, `adapters/swarm_io.rs`, `application/ports/peer_connector.rs`, `application/download.rs`, `domain/peer.rs`, `domain/swarm/peer_registry.rs`, `domain/swarm.rs`.

### Implémenté

- Handshake pur dans le domaine, session commune aux connexions entrantes/sortantes.
- `PeerConnector` expose `Inbound`, `accept()` et `disconnect()` ; `SwarmIO` reçoit et traite les connexions entrantes sans manipuler de socket TCP.
- `TcpPeerListenner` effectue le handshake avec timeout, route par info hash et ferme les torrents inconnus. `SwarmRegistry` contient les senders associés.
- Le domaine conserve `PeerId` et direction, rejette les doublons de même direction et choisit déterministement entre directions opposées. Le nettoyage de l’index par identifiant vérifie qu’il désigne encore l’adresse déconnectée.

### Non branché / à vérifier

- [ ] Démarrer le listener partagé et enregistrer/désenregistrer les swarms sur toutes les sorties. `start_download()` crée actuellement un canal entrant puis abandonne son sender ; le listener privé n’est jamais construit.
- [ ] Partager l’identité et le port annoncés entre listener et connectors au point de composition.
- [ ] Tester les deux ordres d’arrivée des connexions, le remplacement et les événements tardifs. Les événements sont encore identifiés seulement par adresse : une réutilisation d’adresse avant la fin de l’ancienne tâche nécessite une protection explicite.
- [ ] Borner les handshakes entrants simultanés si le listener est exposé : la boucle crée actuellement une tâche par acceptation sans plafond global.

`docs/plan_peer_listener.md` décrit un plan dont plusieurs briques sont désormais réalisées ; son constat initial n’est plus l’état du code. Le travail restant est surtout la composition publique et sa propriété des tâches.

Garder les règles métier dans `Swarm`, les sockets et le disque dans les adapters. `application/download.rs` peut rester le point de composition ; introduire un petit client/session partagé est justifié par le listener global, pas par la recherche d’une architecture hexagonale stricte. Le registre contenant des sockets entrantes peut rester dans les adapters.

## 10. Tests à ajouter avec les corrections

**Priorité : accompagner chaque garantie, pas attendre une réécriture.**

Les 32 tests existants ne couvrent pas les contrats ci-dessus. Ajouter des checks ciblés, en utilisant le domaine pur et des implémentations de ports minimales lorsque cela suffit :

- [ ] Stockage en attente/en échec : aucune publication de complétion prématurée.
- [ ] Reprise : pièces valides conservées, pièces invalides retéléchargées, fichier court préservé puis étendu.
- [ ] Chemins dangereux et symlinks : aucune écriture extérieure.
- [ ] Commandes saturées : déconnexion et libération des assignments, y compris métadonnées.
- [ ] Peer sans réponse et reconnexion/annulation : récupération et fin effective des sessions.
- [ ] Pièce invalide puis valide : reprise et une seule complétion.
- [ ] Upload partagé puis annulé : pas de réponse tardive à un demandeur retiré.
- [ ] Metainfo à taille exactement multiple, hashes incohérents et métadonnées excessives.
- [ ] Magnet : construction depuis `info` et conservation des trackers/hash.
- [ ] Doublons par `PeerId` : direction conservée identique quel que soit l’ordre, déconnexion tardive sans perte du nouveau peer.
- [ ] Shutdown : tâches terminées et résultat disque observé ; événements tracker non répétés.

## Ordre de travail recommandé

1. **Confinement des chemins et invariants metainfo** : empêcher les dégâts et corriger le rejet des torrents à dernière pièce pleine.
2. **Contrat magnet et limites des métadonnées** : rétablir le chemin public sans allocations incontrôlées.
3. **Arrêt global et fermeture disque** : compléter la barrière déjà présente et observer les tâches/erreurs.
4. **Progression réseau** : traiter les commandes de métadonnées refusées et les requêtes sans réponse ; limiter les attentes cumulées.
5. **Composition du listener entrant** : brancher les briques existantes avec identité, port et cycle de vie partagés.
6. **Événements tracker**, puis **mesures de mémoire/scheduler** et optimisations justifiées.

Les tests accompagnent chaque correction. Cette mise à jour ne modifie aucun code Rust.
