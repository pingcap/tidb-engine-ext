#!/bin/bash

set -ex

ffi_path="raftstore-proxy/ffi"
./${ffi_path}/format.sh
# Generate the rust code according to the files
# under "${ffi_path}"
cargo run --package gen-proxy-ffi --bin gen_proxy_ffi
