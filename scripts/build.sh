#!/bin/sh
# 本地复现 release.yml 的构建流程，产出与 CI 一致的 musl 静态二进制：
#   1. ./scripts/theme.sh 下载 pin 住的默认主题（必须在宿主机上跑，
#      cross 容器里不一定有 curl，与 CI 同理）
#   2. web-admin 构建面板 dist/（hub 通过 rust-embed 内嵌它）
#   3. cross 交叉编译 release 产物，拷贝为 monitor-hub-<target>
#
# 用法：
#   ./scripts/build.sh                       # 默认 x86_64（ZimaBoard/ZimaCube）
#   ./scripts/build.sh aarch64-unknown-linux-musl
#   ./scripts/build.sh x86_64-unknown-linux-musl aarch64-unknown-linux-musl
set -eu

cd "$(dirname "$0")/.."

TARGETS="${*:-x86_64-unknown-linux-musl}"

# cross 在容器里执行实际编译，没有 Docker/Podman 会直接失败。
if ! command -v docker >/dev/null 2>&1 && ! command -v podman >/dev/null 2>&1; then
  echo "error: cross 需要 Docker 或 Podman，请先启动其一" >&2
  exit 1
fi
if ! command -v cross >/dev/null 2>&1; then
  echo "installing cross..."
  cargo install --locked cross
fi

# 面板产物必须先于 cargo 存在，否则内嵌进 hub 的是空目录。
(cd web-admin
  [ -d node_modules ] || npm ci
  npm run build)

./scripts/theme.sh

for target in $TARGETS; do
  echo "building $target..."
  cross build --release --locked --target "$target"
  cp "target/$target/release/monitor-hub" "monitor-hub-$target"
  echo "monitor-hub-$target 就绪（Dockerfile 同目录构建镜像用）"
done
