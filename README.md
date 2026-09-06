# Tortue 🐢

Tortue, is a BitTorrent client writtent in Rust.

## CLI:

```txt
Tortue BitTorrent client in Rust

Usage: tortue <COMMAND>

Commands:
  download  Download a torrent
  inspect   Inspect a .torrent file
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## Roadmap

BitTorrent specs: https://www.bittorrent.org/beps/bep_0000.html

| BEP    | Link                                          | Description               | Status                |
| ------ | --------------------------------------------- | ------------------------- | --------------------- |
| BEP 3  | https://www.bittorrent.org/beps/bep_0003.html | Core BitTorrent spec      | Download 🟢 Upload 🔴 |
| BEP 12 | https://www.bittorrent.org/beps/bep_0012.html | Multi Tracker metadata    | 🟢                    |
| BEP 23 | https://www.bittorrent.org/beps/bep_0023.html | Tracker compact peer list | 🟢                    |
| BEP 15 | https://www.bittorrent.org/beps/bep_0015.html | UDP Tracker protocol      | 🟢                    |
| BEP 10 | https://www.bittorrent.org/beps/bep_0010.html | Extension Protocol        | 🟠 WIP                |
