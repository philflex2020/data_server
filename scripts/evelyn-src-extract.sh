#!/usr/bin/env bash
# evelyn-src-extract.sh — package source files from the data_server repo
#
# Run from the data_server repo root on build-dev.
# Output: dist/evelyn-src-YYYYMMDD.tar.gz
#
# Transfer the tar to the build machine, then:
#   tar -xzf evelyn-src-YYYYMMDD.tar.gz
#   bash evelyn-src/scripts/evelyn-k8s-build.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATE="$(date +%Y%m%d)"
OUT_NAME="evelyn-src-${DATE}.tar.gz"
OUT="$REPO_ROOT/dist/$OUT_NAME"
BUILD_DIR="$(mktemp -d /tmp/evelyn-src-XXXXXX)"
STAGE="$BUILD_DIR/evelyn-src"

trap 'rm -rf "$BUILD_DIR"' EXIT

echo "=== Evelyn source extract ==="
echo "    repo:   $REPO_ROOT"
echo "    output: $OUT"
echo

mkdir -p "$REPO_ROOT/dist"
mkdir -p \
    "$STAGE/source" \
    "$STAGE/html/writer" \
    "$STAGE/docker/evelyn-native/configs" \
    "$STAGE/scripts"

# ── C++ source ────────────────────────────────────────────────────────────────
cp -r "$REPO_ROOT/source/parquet_writer"  "$STAGE/source/"

# ── HTML pages ────────────────────────────────────────────────────────────────
cp "$REPO_ROOT/html/writer/evelyn_docker_34.html"           "$STAGE/html/writer/"
cp "$REPO_ROOT/html/parquet_monitor.html"                    "$STAGE/html/"
cp "$REPO_ROOT/html/writer/parquet_viewer.html"              "$STAGE/html/writer/"
cp "$REPO_ROOT/html/writer/writer_design.html"               "$STAGE/html/writer/"
cp "$REPO_ROOT/html/writer/evelyn_real_time_data.html"       "$STAGE/html/writer/"
cp "$REPO_ROOT/html/writer/evelyn_container_design.html"     "$STAGE/html/writer/"
cp "$REPO_ROOT/html/writer/evelyn_writer_project_build.html" "$STAGE/html/writer/"

# ── Native install files ──────────────────────────────────────────────────────
cp "$REPO_ROOT/docker/evelyn-native/bridge_api_native.py"          "$STAGE/docker/evelyn-native/"
cp "$REPO_ROOT/docker/evelyn-native/install.sh"                     "$STAGE/docker/evelyn-native/"
cp "$REPO_ROOT/docker/evelyn-native/configs/flashmq.conf"          "$STAGE/docker/evelyn-native/configs/"
cp "$REPO_ROOT/docker/evelyn-native/configs/writer-native.yaml"    "$STAGE/docker/evelyn-native/configs/"

# ── Build script (self-contained in the tar) ──────────────────────────────────
cp "$REPO_ROOT/scripts/evelyn-k8s-build.sh"  "$STAGE/scripts/"

chmod +x \
    "$STAGE/scripts/"*.sh \
    "$STAGE/docker/evelyn-native/install.sh"

tar -czf "$OUT" -C "$BUILD_DIR" evelyn-src/

echo "=== Done ==="
echo "    $OUT"
echo "    $(du -sh "$OUT" | cut -f1)  $(tar -tzf "$OUT" | wc -l) files"
echo
echo "  Transfer to build machine, then:"
echo "    tar -xzf $OUT_NAME"
echo "    bash evelyn-src/scripts/evelyn-k8s-build.sh [--tag evelyn:1.2.3]"