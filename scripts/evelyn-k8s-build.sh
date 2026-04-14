#!/usr/bin/env bash
# evelyn-k8s-build.sh — compile binaries and produce the deployment tar
#
# Usage:  bash evelyn-src/scripts/evelyn-k8s-build.sh [--tag evelyn:1.2.3]
#
# SRC_DIR defaults to the parent of this script (works when run from extracted
# evelyn-src tar). Override with: SRC_DIR=/path/to/evelyn-src bash ...
#
# Requires: docker (running)
# Output:   dist/evelyn-deploy-YYYYMMDD.tar.gz  (in SRC_DIR/dist/)
set -euo pipefail

# SRC_DIR = extracted evelyn-src directory (contains source/, html/, docker/, scripts/)
SRC_DIR="${EVELYN_SRC:-$(cd "$(dirname "$0")/.." && pwd)}"
DATE="$(date +%Y%m%d)"
OUT_NAME="evelyn-deploy-${DATE}.tar.gz"
OUT="$SRC_DIR/dist/$OUT_NAME"
BUILD_DIR="$(mktemp -d /tmp/evelyn-build-XXXXXX)"
STAGE="$BUILD_DIR/evelyn-deploy"
FLASHMQ_TAG="v1.9.1"
FLASHMQ_CACHE="$SRC_DIR/.cache/flashmq-${FLASHMQ_TAG}"
UBUNTU_IMAGE="ubuntu:24.04"

trap 'echo "[build] cleaning up $BUILD_DIR"; rm -rf "$BUILD_DIR"' EXIT

echo "=== Evelyn build ==="
echo "    src:    $SRC_DIR"
echo "    output: $OUT"
echo "    stage:  $STAGE"
echo

# ── Prerequisite checks ───────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo "ERROR: docker not found — install Docker and ensure the daemon is running"
    exit 1
fi
if ! docker info &>/dev/null; then
    echo "ERROR: Docker daemon not running or permission denied"
    exit 1
fi
if [[ ! -d "$SRC_DIR/source/parquet_writer" ]]; then
    echo "ERROR: $SRC_DIR/source/parquet_writer not found"
    echo "       Run evelyn-src-extract.sh on build-dev first, then:"
    echo "       tar -xzf evelyn-src-YYYYMMDD.tar.gz"
    exit 1
fi

mkdir -p "$SRC_DIR/dist"
mkdir -p \
    "$STAGE/bin" \
    "$STAGE/lib" \
    "$STAGE/html/writer" \
    "$STAGE/configs" \
    "$STAGE/scripts" \
    "$STAGE/logrotate" \
    "$STAGE/k8s"

# ── Step 1: build parquet_writer ─────────────────────────────────────────────
# (defined in its own section below)
build_parquet_writer

# ── Step 2: build FlashMQ ────────────────────────────────────────────────────
build_flashmq

# ── Step 3: collect repo files ────────────────────────────────────────────────
collect_files

# ── Step 4: write Kubernetes manifests ────────────────────────────────────────
write_k8s_manifests

# ── Step 5: package ───────────────────────────────────────────────────────────
tar -czf "$OUT" -C "$BUILD_DIR" evelyn-deploy/
echo
echo "=== Done ==="
echo "    $OUT"
echo "    $(du -sh "$OUT" | cut -f1)  $(tar -tzf "$OUT" | wc -l) files"