#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::todo)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::unimplemented)]
#![deny(clippy::shadow_unrelated)]
#![deny(clippy::shadow_reuse)]
#![deny(clippy::shadow_same)]
#![deny(clippy::too_many_lines)]
#![deny(clippy::too_many_arguments)]
#![deny(clippy::manual_flatten)]
#![deny(clippy::needless_collect)]
#![deny(clippy::redundant_clone)]

pub mod bencode;
pub mod metainfo;
