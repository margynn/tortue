# Fuzzing

Fuzzing targets for torrust_lib using cargo-fuzz.

## Prerequisites

Install cargo-fuzz:

```bash
cargo install cargo-fuzz
```

## Running Fuzz Tests

Run the bencode decoder fuzzer:

```bash
cargo fuzz run bencode
```

Run with specific options:

```bash
# Run for specific duration
cargo +nightly fuzz run bencode -- -max_total_time=60

# Run with specific number of iterations
cargo +nightly fuzz run bencode -- -runs=100000
```

## Targets

- `bencode` - Fuzzes the bencode decoder with arbitrary byte inputs

## Corpus

Fuzz corpus is stored in `corpus/bencode/` and is git-ignored.

## Artifacts

Crash artifacts are stored in `artifacts/bencode/` and should be reviewed when found.
