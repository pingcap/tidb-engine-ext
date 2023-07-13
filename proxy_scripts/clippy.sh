set -uxeo pipefail
export ENGINE_LABEL_VALUE=tiflash
export RUST_BACKTRACE=full
export ENGINE_FEATURES="test-engine-kv-rocksdb test-engine-raft-raft-engine testexport"
rustup component add clippy
# TODO We use --manifest-path as a wordaround.
cargo clippy --package proxy_ffi --features "$ENGINE_FEATURES" --manifest-path proxy_components/proxy_ffi/Cargo.toml --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --package engine_store_ffi --features "$ENGINE_FEATURES" --features engine_tiflash/testexport --manifest-path proxy_components/engine_store_ffi/Cargo.toml --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --package proxy_tests --features "$ENGINE_FEATURES" --features engine_tiflash/testexport --manifest-path proxy_tests/Cargo.toml --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc
cargo clippy --package proxy_server --features "$ENGINE_FEATURES" --features engine_tiflash/testexport --manifest-path proxy_components/proxy_server/Cargo.toml --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq
cargo clippy --package mock-engine-store --features "$ENGINE_FEATURES" --features engine_tiflash/testexport --manifest-path proxy_components/mock-engine-store/Cargo.toml --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::needless_borrow -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments -A incomplete_features
cargo clippy --package engine_tiflash --features "$ENGINE_FEATURES" --manifest-path proxy_components/engine_tiflash/Cargo.toml  --no-deps -- -Dwarnings -A clippy::result_large_err -A clippy::clone_on_copy -A clippy::upper_case_acronyms -A clippy::missing_safety_doc -A clippy::derive_partial_eq_without_eq -A clippy::redundant_clone -A clippy::too_many_arguments