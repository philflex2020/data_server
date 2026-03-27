#!/usr/bin/env bash
# install.sh — native deployment of Evelyn demo stack
# Run as root or a user with sudo access.
#
# Usage:  bash install.sh [--parquet-path /path/to/data]
#
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARQUET_PATH="/data/parquet-evelyn"

while [[ $# -gt 0 ]]; do
  case $1 in
    --parquet-path) PARQUET_PATH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "=== Evelyn native install ==="
echo "    parquet path: $PARQUET_PATH"

# ── Arrow / Parquet runtime libs ─────────────────────────────────────────────
if ls "$SCRIPT_DIR"/lib/*.deb 1>/dev/null 2>&1; then
  echo "Installing Arrow/Parquet .deb files..."
  sudo apt-get install -y "$SCRIPT_DIR"/lib/*.deb
else
  echo "No .deb files in lib/ — assuming Arrow/Parquet already installed"
fi

# ── Binaries ─────────────────────────────────────────────────────────────────
echo "Installing binaries to /usr/local/bin/..."
sudo install -m 755 "$SCRIPT_DIR"/bin/flashmq         /usr/local/bin/flashmq
sudo install -m 755 "$SCRIPT_DIR"/bin/parquet-writer   /usr/local/bin/parquet-writer
sudo install -m 755 "$SCRIPT_DIR"/bridge_api_native.py /usr/local/bin/bridge_api_native.py

# ── Config files ──────────────────────────────────────────────────────────────
echo "Installing configs to /etc/evelyn/..."
sudo mkdir -p /etc/evelyn
sudo install -m 644 "$SCRIPT_DIR"/configs/flashmq.conf     /etc/evelyn/flashmq.conf
sudo install -m 644 "$SCRIPT_DIR"/configs/writer-native.yaml /etc/evelyn/writer.yaml

# Update parquet path in writer config
sudo sed -i "s|base_path:.*|base_path: $PARQUET_PATH|" /etc/evelyn/writer.yaml

# ── Data dirs ─────────────────────────────────────────────────────────────────
echo "Creating data directories..."
sudo mkdir -p "$PARQUET_PATH"
sudo chown "$USER":"$USER" "$PARQUET_PATH"
sudo mkdir -p /var/lib/flashmq /var/lib/parquet-writer/wal
sudo chown "$USER":"$USER" /var/lib/parquet-writer /var/lib/parquet-writer/wal

# ── systemd units ─────────────────────────────────────────────────────────────
echo "Installing systemd units..."
sudo install -m 644 "$SCRIPT_DIR"/systemd/flashmq.service      /etc/systemd/system/
sudo install -m 644 "$SCRIPT_DIR"/systemd/parquet-writer.service /etc/systemd/system/
sudo install -m 644 "$SCRIPT_DIR"/systemd/bridge-api.service    /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable flashmq parquet-writer bridge-api

echo ""
echo "=== Install complete ==="
echo "  Start:   sudo systemctl start flashmq parquet-writer bridge-api"
echo "  Status:  systemctl status flashmq parquet-writer"
echo "  Health:  curl http://localhost:8771/health"
echo "  Mgmt:    curl http://localhost:8772/status"
echo ""
echo "  To set a bridge to your EMS broker:"
echo "    curl -X POST http://localhost:8772/bridge \\"
echo "         -H 'Content-Type: application/json' \\"
echo "         -d '{\"address\":\"<EMS_IP>\",\"port\":1883,\"topic\":\"ems/#\"}'"
