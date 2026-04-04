# Torrust

Implementation:

```
bencode/
metainfo/
tracker/
peer/
protocol/
piece_manager/
storage/
```

Steps:

- ✅ bencode
- ✅ metainfo + info_hash
- ✅ tracker (HTTP + compact peers)
- ✅ TCP connection
- ✅ handshake
- ✅ read bitfield
- send interested
- wait unchoke
- request 1 block
- receive + verify
