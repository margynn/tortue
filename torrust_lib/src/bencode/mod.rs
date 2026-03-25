//! Bencode encoding and decoding functionality.
//! 
//! This module provides the core Bencode data types and functions for
//! serializing and deserializing Bencode data structures.

pub mod encoder;
pub mod decoder;

use std::fmt;

/// The Bencode data type enum representing all valid Bencode values.
#[derive(Debug, Clone, PartialEq)]
pub enum Bencode {
    /// A signed 64-bit integer
    Int(i64),
    /// A byte string
    Bytes(Vec<u8>),
    /// A list of Bencode values
    List(Vec<Bencode>),
    /// A dictionary mapping byte strings to Bencode values
    Dict(Vec<(&'static [u8], Bencode)>),
}

impl fmt::Display for Bencode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Bencode::Int(n) => write!(f, "i{}e", n),
            Bencode::Bytes(bytes) => {
                write!(f, "{}:", bytes.len())?;
                for b in bytes {
                    write!(f, "{:02x}", b)?;
                }
                Ok(())
            }
            Bencode::List(items) => {
                write!(f, "l")?;
                for item in items {
                    write!(f, "{}", item)?;
                }
                write!(f, "e")
            }
            Bencode::Dict(entries) => {
                write!(f, "d")?;
                for (key, value) in entries {
                    write!(f, "{}{}", key, value)?;
                }
                write!(f, "e")
            }
        }
    }
}

/// Result type for Bencode operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during Bencode encoding or decoding
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Invalid integer format
    InvalidInt,
    /// Invalid byte string length prefix
    InvalidStringLength,
    /// Invalid list format
    InvalidList,
    /// Invalid dictionary format
    InvalidDict,
    /// Unexpected end of input
    UnexpectedEnd,
    /// Invalid character in input
    InvalidCharacter(char),
    /// Key not found in dictionary
    KeyNotFound(String),
    /// Type mismatch when accessing value
    TypeMismatch,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidInt => write!(f, "invalid integer format"),
            Error::InvalidStringLength => write!(f, "invalid string length prefix"),
            Error::InvalidList => write!(f, "invalid list format"),
            Error::InvalidDict => write!(f, "invalid dictionary format"),
            Error::UnexpectedEnd => write!(f, "unexpected end of input"),
            Error::InvalidCharacter(c) => write!(f, "invalid character: {}", c),
            Error::KeyNotFound(key) => write!(f, "key not found: {}", key),
            Error::TypeMismatch => write!(f, "type mismatch"),
        }
    }
}

impl std::error::Error for Error {}

pub use encoder::{encode, encode_int, encode_bytes, encode_list, encode_dict};
pub use decoder::{decode, decode_int, decode_bytes, decode_list, decode_dict};
