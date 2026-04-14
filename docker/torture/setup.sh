#!/usr/bin/env bash
# setup.sh — one-shot setup for the torture test on phil-256g
# Run from the repo root:  bash docker/torture/setup.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TORTURE_DIR="$REPO_ROOT/docker/torture"

echo "=== torture test setup ==="
echo "repo: $REPO_ROOT"

# ── 1. Copy Arrow/Parquet .debs from host apt cache ─────────────────────────
echo
echo "[1/4] copying Arrow/Parquet .debs from host apt cache..."
mkdir -p "$TORTURE_DIR/debs"
DEBS=(
    libarrow2300
    libarrow-dev
    libparquet2300
    libparquet-dev
    apache-arrow-apt-source
)
for pkg in "${DEBS[@]}"; do
    f=$(find /var/cache/apt/archives -name "${pkg}_*.deb" 2>/dev/null | head -1)
    if [ -n "$f" ]; then
        cp "$f" "$TORTURE_DIR/debs/"
        echo "  copied: $(basename $f)"
    else
        echo "  MISSING: $pkg — run 'sudo apt-get install -y $pkg' first"
    fi
done

# ── 2. Build the writer Docker image ────────────────────────────────────────
echo
echo "[2/4] building parquet-writer:latest Docker image..."
docker build \
    -f "$TORTURE_DIR/Dockerfile.writer" \
    -t parquet-writer:latest \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    "$REPO_ROOT"
echo "  image built: parquet-writer:latest"

# ── 3. Create stress runner torture config ──────────────────────────────────
echo
echo "[3/4] stress runner config already at docker/torture/configs/stress_torture.yaml"

# ── 4. Start the stack ──────────────────────────────────────────────────────
echo
echo "[4/4] starting docker-compose stack..."
cd "$TORTURE_DIR"
docker compose up -d
docker compose ps

echo
echo "=== stack is up ==="
echo
echo "Health endpoints:"
for port in 8771 8772 8773 8774; do
    label="writer-$(echo $((port - 8770)) | tr '1234' 'abcd')"
    status=$(curl -s "http://localhost:${port}/health" 2>/dev/null || echo '{"error":"not up yet"}')
    echo "  $label :$port  $status"
done

echo
echo "Useful commands:"
echo "  Watch all writers:  watch -n2 'for p in 8771 8772 8773 8774; do printf \":%d \"; curl -s localhost:\$p/health; echo; done'"
echo "  Broker logs:        docker compose logs -f broker"
echo "  Inject a fault:     SCENARIO=broker_kill docker compose --profile fault run fault"
echo "  Run all faults:     SCENARIO=all CYCLE_SECONDS=60 docker compose --profile fault run fault"
echo "  Validate output:    python3 docker/torture/scripts/torture_validate.py"
echo "  Stop everything:    docker compose down"
