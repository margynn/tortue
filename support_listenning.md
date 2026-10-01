dans /Users/martin.guyard/Development/tortue/tortue_lib/src/adapters/peer_io.rs on a definit
PeerListenner

Le challenge c'est de pouvoir accepter des connection entrantes.
Peer IO est instancier par le swarmIO via peerConnector.

PeerIO contient le handshake. Il faut peut etre le passer dans le domaine totalement.

Architecture:
Listenner qui demare avec le reste du programme.
Passer a swarmIO (injecter)

Cahier des charges:

- accepter les connections entrantes -> voir PeerListenner
- decoder le handshake
- regarder si on connait le metainfo: Il faut un registre avec tout les metainfo connus (donc tout les swarms qui existent)
  - Si inconnu -> abandonne la connection
- sinon on repond au handshake
  - si le peer_id existe deja dans le swarm on abandonne
- on ajoute le peer au swarm

Idealement il faudrait avoir une approche similare a PeerIO. Le probleme de PeerIO est qu'il est concu pour faire la reconnection du peer. Ca n'a pas de sens lorsqu'on accepte une connection. Donc il faut probablement splitter la partie de peer io qui fait la connection du reste.
