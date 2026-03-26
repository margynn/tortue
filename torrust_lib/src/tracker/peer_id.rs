#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 20]);

impl PeerId {
    /// Create a new PeerId from 20 bytes
    pub fn new(bytes: [u8; 20]) -> Self {
        PeerId(bytes)
    }

    // /// Generate a random PeerId with a given client prefix
    // pub fn generate(prefix: &str) -> Self {
    //     assert!(prefix.len() <= 8, "Prefix must be <= 8 bytes");
    //     let mut bytes = [0u8; 20];
    //     bytes[..prefix.len()].copy_from_slice(prefix.as_bytes());
    //     getrandom::getrandom(&mut bytes[prefix.len()..]).unwrap();
    //     PeerId(bytes)
    // }
}

impl AsRef<[u8]> for PeerId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
