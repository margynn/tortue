BEP 10 — Extension Protocol

BEP 10 permet à deux peers BitTorrent de négocier des extensions au protocole BitTorrent standard.

L'extension la plus importante pour ton client est ut_metadata, notamment pour récupérer les métadonnées d'un torrent à partir d'un magnet link.

1. Détecter le support

Le support de BEP 10 est annoncé dans le handshake BitTorrent classique via les reserved bytes.

Tester :

reserved[5] & 0x10 != 0

Si le bit est activé, le peer supporte les extended messages.

2. Le message extended

BEP 10 ajoute le message BitTorrent :

message ID = 20

Structure :

+----------------+-------------+----------------+----------+
| length (4 B) | ID = 20 | extension ID | payload |
+----------------+-------------+----------------+----------+
length : longueur du message
20 : identifie un message extended
extension ID : identifie l'extension
payload : données de l'extension 3. Extended handshake

Après le handshake BitTorrent, un peer peut envoyer un message extended avec :

extension ID = 0

L'extension 0 est réservée au handshake BEP 10.

Le payload est un dictionnaire bencoded.

Exemple :

{
"m": {
"ut_metadata": 1,
"ut_pex": 2
}
}

Cela signifie :

ut_metadata → ID 1
ut_pex → ID 2
Attention aux IDs

Les IDs sont locaux à chaque connexion.

Il ne faut donc pas supposer que :

ut_metadata = 1

Un autre peer peut parfaitement annoncer :

{
"m": {
"ut_metadata": 5,
"ut_pex": 8
}
}

Ton client doit utiliser l'ID annoncé par le peer.

4. État à conserver par peer

Par exemple :

struct PeerExtensions {
enabled: bool,
extensions: HashMap<String, u8>,
}

Après réception du handshake :

"ut_metadata" → 5
"ut_pex" → 8 5. Envoyer une extension

Si le peer annonce :

ut_metadata → 5

un message ut_metadata utilise :

+--------+----+----+---------+
| length | 20 | 5 | payload |
+--------+----+----+---------+

Le 5 est l'ID attribué à ut_metadata par ce peer.

6. Extensions inconnues

Si le peer annonce une extension que ton client ne connaît pas :

{
"m": {
"ut_metadata": 5,
"ut_pex": 8,
"some_future_extension": 12
}
}

Il suffit de l'ignorer.

7. Extensions importantes

BEP 10 ne définit pas lui-même ut_metadata ou ut_pex.

Il fournit uniquement le mécanisme permettant de négocier et transporter des extensions.

Pour ton client :

BEP 10
│
├── ut_metadata
│ └── récupérer les métadonnées d'un magnet
│
└── ut_pex
└── échanger des peers 8. Implémentation minimale

1. Détection
   if reserved[5] & 0x10 != 0 {
   // Peer compatible BEP 10
   }
2. Réception
   match extended_id {
   0 => handle_extension_handshake(payload),
   id => handle_extension(id, payload),
   }
3. Extended handshake

Parser le payload comme du bencode et récupérer :

m → extension_name → extension_id

Puis stocker les associations par peer.

4. Envoi

Pour envoyer une extension :

length
20
extension_id
payload 9. Flux complet
TCP connection
│
▼
BitTorrent handshake
│
│ reserved[5] & 0x10
▼
BEP 10 extended handshake
│
│ "m": {
│ "ut_metadata": 3,
│ "ut_pex": 7
│ }
▼
Stocker les IDs
│
├── ut_metadata → 3
│
└── ut_pex → 7
À retenir

Pour implémenter BEP 10, il te faut essentiellement :

Détecter le bit BEP 10 dans le handshake.
Gérer le message extended (ID = 20).
Gérer l'extended handshake (extension ID = 0).
Mémoriser les IDs d'extensions par peer.
Utiliser ces IDs pour envoyer les extensions correspondantes.

BEP 10 est donc essentiellement une couche de négociation située au-dessus du protocole peer BitTorrent.
