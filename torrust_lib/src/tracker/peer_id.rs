#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 20]);

impl PeerId {
    /// Create a new PeerId from 20 bytes
    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    /// Generate a random PeerId with a given client prefix (up to 8 bytes)
    pub fn generate(prefix: &str) -> Self {
        // if prefix.len() > 8 {
        //     return Err("Prefix must be <= 8 bytes".to_string());
        // }
        let mut bytes = [0u8; 20];
        bytes[..prefix.len()].copy_from_slice(prefix.as_bytes());
        // Fill the rest with random bytes; panic on failure
        getrandom::fill(&mut bytes[prefix.len()..])
            .expect("Failed to generate random PeerId bytes");
        Self(bytes)
    }
}

impl AsRef<[u8]> for PeerId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
