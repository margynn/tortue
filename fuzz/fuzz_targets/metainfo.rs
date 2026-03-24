#![no_main]

use libfuzzer_sys::fuzz_target;
use torrust_lib::metainfo;

fuzz_target!(|data: &[u8]| {
    let _ = metainfo::decode(data);
});
