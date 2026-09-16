#!/bin/sh
# 构建 plugin.tar.gz：hub 的上传接口（POST /api/plugins）收的就是这个包。
# 需要一次性的 `rustup target add wasm32-unknown-unknown`。
set -e
cd "$(dirname "$0")"
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/finance_stats.wasm plugin.wasm
tar czf plugin.tar.gz plugin.toml plugin.wasm
echo "built plugin.tar.gz（含 plugin.toml + plugin.wasm，$(wc -c < plugin.wasm | tr -d ' ') 字节的模块）"
