//! A pure Rust implementation of the Bencode encoding format.
//! 
//! Bencode is a data serialization format used by BitTorrent clients.
//! It supports integers, byte strings, lists, and dictionaries.
//!
//! # Example
//! ```
//! use torrust_lib::bencode::{Bencode, encode, decode};
//!
//! // Create a simple dictionary
//! let dict = Bencode::Dict(vec![
//!     (b"name".as_ref(), Bencode::Bytes(b"John")),
//!     (b"age".as_ref(), Bencode::Int(25)),
//! ]);
//! 
//! // Encode to bytes
//! let encoded = encode(&dict).unwrap();
//! assert_eq!(encoded, b"d4:name5:Johni25ee");
//! ```

pub mod bencode;

pub use bencode::{Bencode, decode, encode};
