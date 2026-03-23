# BitTorrent Protocol Specification (BEP 3)

## Overview

BitTorrent is a peer-to-peer protocol for distributing files. A distribution involves:
- A web server hosting a `.torrent` file
- A tracker coordinating peers
- Peers downloading and uploading pieces of the file

---

## Bencoding

### Types

**String**
- Format: `<length>:<data>`
- Example: `4:spam` → "spam"

**Integer**
- Format: `i<number>e`
- Examples: `i3e` → 3, `i-3e` → -3
- Rules:
  - No leading zeros (`i03e` invalid)
  - `i-0e` is invalid
  - `i0e` is valid

**List**
- Format: `l<items>e`
- Example: `l4:spam4:eggse` → ["spam", "eggs"]

**Dictionary**
- Format: `d<key><value>...e`
- Keys:
  - Must be strings
  - Must be sorted lexicographically (raw byte order)
- Example:
  - `d3:cow3:moo4:spam4:eggse` → {"cow": "moo", "spam": "eggs"}

---

## Metainfo Files (.torrent)

Bencoded dictionary with:

### Top-level keys

- `announce`: tracker URL
- `info`: dictionary (see below)

All text strings must be UTF-8 encoded.

---

## Info Dictionary

### Common fields

- `name`: suggested file or directory name (UTF-8)
- `piece length`: size of each piece in bytes (commonly 2^18 = 256 KiB)
- `pieces`: concatenated SHA1 hashes (20 bytes each)

### Single-file mode

- `length`: file size in bytes

### Multi-file mode

- `files`: list of dictionaries:
  - `length`: file size in bytes
  - `path`: list of UTF-8 strings (directory components + filename)

Files are logically concatenated in listed order.

---

## Tracker Protocol (HTTP)

### Request Parameters

- `info_hash`: 20-byte SHA1 of raw bencoded `info` (must not re-encode invalid data)
- `peer_id`: 20-byte client identifier
- `port`: listening port
- `uploaded`: total bytes uploaded
- `downloaded`: total bytes downloaded
- `left`: bytes remaining
- `ip` (optional): client IP
- `event` (optional): `started`, `completed`, `stopped`

### Response

Bencoded dictionary:

**Failure case**
- `failure reason`: error string

**Success case**
- `interval`: seconds between requests
- `peers`: either:
  - List of dictionaries:
    - `peer id`
    - `ip`
    - `port`
  - Or compact format (see BEP 23)

---

## Peer Protocol

### Transport

- TCP or uTP

### State

Each connection maintains:
- Choked / unchoked
- Interested / not interested

Connection starts as:
- Choked
- Not interested

Data flows when:
- One side is interested
- The other is unchoked

---

## Handshake

Structure:

1. `<pstrlen>` (1 byte, value = 19)
2. `"BitTorrent protocol"` (19 bytes)
3. 8 reserved bytes (all zero)
4. `info_hash` (20 bytes)
5. `peer_id` (20 bytes)

Rules:
- If `info_hash` differs → close connection
- If `peer_id` mismatch → close connection

---

## Message Framing

- Messages: `<length prefix><message>`
- Length prefix: 4-byte big-endian
- `length = 0` → keepalive

---

## Message Types

| ID | Name           | Payload |
|----|----------------|---------|
| 0  | choke          | none    |
| 1  | unchoke        | none    |
| 2  | interested     | none    |
| 3  | not interested | none    |
| 4  | have           | piece index |
| 5  | bitfield       | bitfield |
| 6  | request        | index, begin, length |
| 7  | piece          | index, begin, block |
| 8  | cancel         | index, begin, length |

---

## Message Details

**bitfield**
- Sent first (optional if empty)
- Bit per piece (MSB first)

**have**
- Announces completed piece index

**request**
- `(index, begin, length)`
- Typical length: 2^14 (16 KiB)

**piece**
- `(index, begin, block)`

**cancel**
- Same as request
- Used in endgame mode

---

## Piece Selection

- Pieces are requested in random order
- Improves distribution across peers

---

## Choking Algorithm

Goals:
- Limit simultaneous uploads
- Avoid rapid state changes
- Prefer peers providing data
- Explore new peers (optimistic unchoke)

Behavior:
- Re-evaluate every 10 seconds
- Unchoke top 4 peers by download rate (if interested)
- One optimistic unchoke (rotates every 30 seconds)
- Completed peers use upload rate instead

---

## Notes

- All integers in peer protocol are 4-byte big-endian
- Requests should be pipelined for performance
- Invalid `.torrent` files must not be re-encoded when computing `info_hash`
