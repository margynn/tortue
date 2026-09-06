# BEP 9 — Magnet Link Download Architecture

## Overview

Two entry points for the `download` command:

- `.torrent` file → metadata already available, go straight to piece download
- magnet link → fetch metadata first via BEP 9, then same piece download flow

## CLI

```
tortue download <path|magnet>   # auto-detect
tortue download magnet:?xt=urn:btih:<hash>&tr=<tracker>&dn=<name>
tortue download ./file.torrent
```

`main.rs`: the `path: PathBuf` argument becomes `source: String`, detected at runtime:

- starts with `magnet:` → parse as magnet
- otherwise → read as file path

## Magnet Link Format

```
magnet:?xt=urn:btih:<info_hash_hex_or_base32>
       &tr=<tracker_url>      (0 or more, repeatable)
       &dn=<display_name>     (optional)
```

Minimum required: `xt` (info hash). Trackers optional (DHT fallback, not implemented yet).

Parsed into:

```rust
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub trackers: Vec<String>,
    pub name: Option<String>,
}
```

## Download Flow

### With .torrent (current)

```
torrent bytes
    → Metainfo::try_from(&bytes)
    → Arc<Metainfo>
    → Pool + TrackerIO + PeerConnector
    → piece download
```

### With magnet link (new)

```
MagnetLink { info_hash, trackers, .. }
    → MetadataFetcher::run(info_hash, trackers)   ← new
    → Arc<Metainfo>                                ← same from here
    → Pool + TrackerIO + PeerConnector
    → piece download
```

The application layer (`download.rs`) handles the branching. Once `Arc<Metainfo>` is obtained, the rest is identical.

## MetadataFetcher

New adapter: `adapters/metadata_io.rs`

Responsibilities:

1. Contact trackers with `info_hash` to get peers
2. Connect to peers via `TcpPeerConnector`
3. On BEP 10 handshake: check if peer supports `ut_metadata` + read `metadata_size`
4. Send `UtMetadataMessage::Request { piece }` for each piece 0..N
5. Collect `Data` responses into a buffer `Vec<Option<Vec<u8>>>`
6. Once all pieces received: concatenate, SHA1 validate against `info_hash`
7. On success: return `Arc<Metainfo>`
8. On validation failure: discard buffer, retry with another peer

### State machine (domain/metadata_fetch.rs)

```rust
pub enum MetadataInput {
    PeerConnected { addr, peer_id, extensions },
    PeerDisconnected(addr),
    MessageReceived { addr, message },
}

pub enum MetadataOutput {
    ConnectPeer(SocketAddr),
    SendToPeer { addr, message },
    Done(Vec<u8>),  // raw validated info bytes
}
```

The fetcher is complete once `Done` is emitted. The caller validates SHA1 and builds `Metainfo`.

### Key constraints

- A peer must have `metadata_size` in their BEP 10 handshake to be usable
- Only request from peers that have confirmed `ut_metadata` support
- Reject pieces that don't validate (partial SHA1 not possible — validate only on completion)
- If a peer disconnects mid-fetch, re-request missing pieces from remaining peers

## New files

| File                                   | Role                                                |
| -------------------------------------- | --------------------------------------------------- |
| `domain/metadata_fetch.rs`             | Pure state machine for metadata fetching            |
| `adapters/metadata_io.rs`              | Async runner: peers + tracker + message loop        |
| `tortue_lib/src/application/magnet.rs` | `MagnetLink` parse + `fetch_metadata()` entry point |

## Modified files

| File                                     | Change                                                      |
| ---------------------------------------- | ----------------------------------------------------------- |
| `src/main.rs`                            | Accept `source: String`, detect magnet vs file              |
| `tortue_lib/src/application/download.rs` | Accept `MagnetLink` or `.torrent` bytes, branch before pool |
| `tortue_lib/src/lib.rs`                  | Export `download_magnet` or unify `download` signature      |
