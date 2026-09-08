use super::torrent::InfoHash;
use percent_encoding::percent_decode_str;

#[derive(Debug)]
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub trackers: Vec<String>,
    pub name: Option<String>,
    pub peers: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing or invalid magnet: scheme")]
    InvalidScheme,

    #[error("no supported xt (urn:btih) parameter found")]
    MissingXt,

    #[error("invalid info hash: {0}")]
    InvalidInfoHash(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl TryFrom<&str> for MagnetLink {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        let query = s.strip_prefix("magnet:?").ok_or(Error::InvalidScheme)?;

        let mut info_hash: Option<InfoHash> = None;
        let mut trackers = Vec::new();
        let mut name = None;
        let mut peers = Vec::new();

        for param in query.split('&') {
            let Some((key, raw_value)) = param.split_once('=') else {
                continue;
            };
            let value = percent_decode_str(raw_value)
                .decode_utf8_lossy()
                .into_owned();

            match key {
                "xt" => {
                    if info_hash.is_none() {
                        if let Some(hash) = value.strip_prefix("urn:btih:") {
                            info_hash = Some(parse_btih(hash)?);
                        }
                        // urn:btmh: (v2 multihash) is skipped; btih takes priority
                    }
                },
                "dn" => name = Some(value),
                "tr" => trackers.push(value),
                "x.pe" => peers.push(value),
                _ => {},
            }
        }

        Ok(MagnetLink {
            info_hash: info_hash.ok_or(Error::MissingXt)?,
            trackers,
            name,
            peers,
        })
    }
}

fn parse_btih(hash: &str) -> Result<InfoHash> {
    match hash.len() {
        40 => {
            let bytes = hex::decode(hash).map_err(|e| Error::InvalidInfoHash(e.to_string()))?;
            let arr: [u8; 20] = bytes
                .try_into()
                .map_err(|_| Error::InvalidInfoHash("expected 20 bytes".into()))?;
            Ok(InfoHash::from(arr))
        },
        32 => {
            let bytes = base32_decode(hash)?;
            let arr: [u8; 20] = bytes.try_into().map_err(|_| {
                Error::InvalidInfoHash("base32 decode did not yield 20 bytes".into())
            })?;
            Ok(InfoHash::from(arr))
        },
        _ => Err(Error::InvalidInfoHash(format!(
            "expected 40 hex or 32 base32 chars, got {}",
            hash.len()
        ))),
    }
}

// RFC 4648 base32 decode (no padding required for 32-char info hashes)
fn base32_decode(s: &str) -> Result<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    let mut bits: u32 = 0;
    let mut bit_count: u32 = 0;
    let mut output = Vec::with_capacity(20);

    for byte in s.bytes() {
        let upper = byte.to_ascii_uppercase();
        let val = ALPHABET.iter().position(|&b| b == upper).ok_or_else(|| {
            Error::InvalidInfoHash(format!("invalid base32 character: {}", byte as char))
        })? as u32;

        bits = (bits << 5) | val;
        bit_count += 5;

        if bit_count >= 8 {
            bit_count -= 8;
            output.push((bits >> bit_count) as u8);
            bits &= (1 << bit_count) - 1;
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_v1_hex_info_hash() {
        let uri = "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=Test+Torrent&tr=udp%3A%2F%2Ftracker.example.com%3A80";
        let magnet = MagnetLink::try_from(uri).unwrap();
        assert_eq!(
            magnet.info_hash.as_ref(),
            &hex::decode("dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c").unwrap()[..]
        );
        assert_eq!(magnet.name.as_deref(), Some("Test+Torrent"));
        assert_eq!(magnet.trackers, vec!["udp://tracker.example.com:80"]);
        assert!(magnet.peers.is_empty());
    }

    #[test]
    fn parses_v1_base32_info_hash() {
        // 20 zero bytes in RFC 4648 base32 (no padding) = 32 'A' characters
        let uri = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let magnet = MagnetLink::try_from(uri).unwrap();
        assert_eq!(magnet.info_hash.as_ref(), &[0u8; 20][..]);
    }

    #[test]
    fn parses_multiple_trackers_and_peers() {
        let uri = "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&tr=udp%3A%2F%2Ftracker1.example.com%3A80&tr=udp%3A%2F%2Ftracker2.example.com%3A80&x.pe=192.168.1.1%3A6881";
        let magnet = MagnetLink::try_from(uri).unwrap();
        assert_eq!(magnet.trackers.len(), 2);
        assert_eq!(magnet.peers, vec!["192.168.1.1:6881"]);
    }

    #[test]
    fn btmh_only_returns_missing_xt() {
        let uri = "magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad475f4f69f4";
        let err = MagnetLink::try_from(uri).unwrap_err();
        assert!(matches!(err, Error::MissingXt));
    }

    #[test]
    fn missing_scheme_returns_error() {
        let err = MagnetLink::try_from("not-a-magnet").unwrap_err();
        assert!(matches!(err, Error::InvalidScheme));
    }
}
