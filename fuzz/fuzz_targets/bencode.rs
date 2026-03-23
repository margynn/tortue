#![no_main]

use libfuzzer_sys::fuzz_target;
use torrust_lib::bencode;

fuzz_target!(|data: &[u8]| {
    let _ = bencode::decode(data,);
});
