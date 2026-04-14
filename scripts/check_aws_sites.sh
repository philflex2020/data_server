#!/usr/bin/env bash
# check_sites.sh — config-driven health check for data server sites
#
# All port and HTTP probes run on the remote host via SSH, so they work
# even when ports are firewalled (e.g. AWS security groups only allow :22).
#
# Usage:
#   ./scripts/check_sites.sh                          # uses config/sites_config.yaml
#   ./scripts/check_sites.sh /path/to/other.yaml      # override config
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${1:-$REPO_ROOT/config/sites_config.yaml}"

# ── colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { printf "  ${GREEN}✓${RESET}  %s\n" "$*"; }
fail() { printf "  ${RED}✗${RESET}  %s\n" "$*"; }
warn() { printf "  ${YELLOW}~${RESET}  %s\n" "$*"; }
info() { printf "  ${CYAN}·${RESET}  %s\n" "$*"; }
hdr()  { printf "\n${BOLD}${CYAN}── %s ──${RESET}\n" "$*"; }

# ── preflight ────────────────────────────────────────────────────────────────
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: config not found: $CONFIG_FILE" >&2; exit 1
fi
if ! python3 -c "import yaml" 2>/dev/null; then
  echo "Error: python3 yaml module required (pip install pyyaml)" >&2; exit 1
fi

# ── parse config → shell variables ───────────────────────────────────────────
eval "$(python3 - "$CONFIG_FILE" << 'PYEOF'
import yaml, sys

with open(sys.argv[1]) as f:
    cfg = yaml.safe_load(f)

sites = cfg.get('sites', [])
print(f'SITE_COUNT={len(sites)}')

def q(v):
    return str(v if v is not None else '').replace("'", "'\\''")

for i, s in enumerate(sites):
    for k in ['name', 'host', 'user', 'type', 'ssh_key', 'ssh_password',
              'aws_region', 's3_parquet_bucket', 's3_parquet_prefix', 'ecs_cluster']:
        print(f"SITE_{i}_{k.upper()}='{q(s.get(k, ''))}'")
    # space-separated lists
    print(f"SITE_{i}_PARQUET_DIRS='{' '.join(s.get('parquet_dirs', []))}'")
    print(f"SITE_{i}_SYSTEMD='{' '.join(s.get('systemd_services', []))}'")
    print(f"SITE_{i}_ECS_SERVICES='{' '.join(s.get('ecs_services', []))}'")
PYEOF
)"

# ── SSH helpers ───────────────────────────────────────────────────────────────

# ssh_run HOST USER KEY PASSWORD COMMAND...
# Runs a single quoted command on the remote host.
ssh_run() {
  local host=$1 user=$2 key=$3 password=$4
  shift 4
  local opts="-o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no"
  if [[ -n "$password" ]]; then
    if ! command -v sshpass &>/dev/null; then
      echo "error: sshpass required for password auth (apt install sshpass)" >&2; return 1
    fi
    sshpass -p "$password" ssh $opts "${user}@${host}" "$@"
  elif [[ -n "$key" ]]; then
    ssh $opts -i "${key/#\~/$HOME}" "${user}@${host}" "$@"
  else
    ssh $opts "${user}@${host}" "$@"
  fi
}

# ssh_script HOST USER KEY PASSWORD << 'SCRIPT'
# Sends a heredoc script to the remote host via stdin.
ssh_script() {
  local host=$1 user=$2 key=$3 password=$4
  local opts="-o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no"
  if [[ -n "$password" ]]; then
    sshpass -p "$password" ssh $opts "${user}@${host}" bash
  elif [[ -n "$key" ]]; then
    ssh $opts -i "${key/#\~/$HOME}" "${user}@${host}" bash
  else
    ssh $opts "${user}@${host}" bash
  fi
}

# ── remote probe helpers ──────────────────────────────────────────────────────

# Check if a port is listening on the remote host (via SSH nc)
r_tcp() {
  local host=$1 user=$2 key=$3 password=$4 port=$5 label=$6
  if ssh_run "$host" "$user" "$key" "$password" "nc -z -w3 localhost $port" 2>/dev/null; then
    ok "$label (:$port)"
  else
    fail "$label (:$port) — not listening"
  fi
}

# HTTP check via curl on the remote host
r_http() {
  local host=$1 user=$2 key=$3 password=$4 url=$5 label=$6 ok_codes=${7:-"2"}
  local code
  code=$(ssh_run "$host" "$user" "$key" "$password" \
    "curl -s -o /dev/null -w '%{http_code}' --max-time 5 '$url'" 2>/dev/null || echo "000")
  if [[ "$code" =~ ^[$ok_codes] ]]; then
    ok "$label ($code)"
  elif [[ "$code" == "000" ]]; then
    fail "$label — no response"
  else
    warn "$label — HTTP $code"
  fi
}

# HTTP + JSON extraction via curl+python3 on the remote host
r_http_json() {
  local host=$1 user=$2 key=$3 password=$4 url=$5 label=$6 py_expr=$7
  local body
  body=$(ssh_run "$host" "$user" "$key" "$password" \
    "curl -sf --max-time 5 '$url'" 2>/dev/null || true)
  if [[ -z "$body" ]]; then
    fail "$label — no response"
  else
    local val
    val=$(echo "$body" | python3 -c "$py_expr" 2>/dev/null || echo "$body" | head -c 80)
    ok "$label — $val"
  fi
}

# Parquet directory counts via SSH
r_parquet_dirs() {
  local host=$1 user=$2 key=$3 password=$4
  local dirs=$5   # space-separated paths
  [[ -z "$dirs" ]] && return

  printf "\n  Parquet data dirs:\n"
  ssh_script "$host" "$user" "$key" "$password" << REMOTE
for dir in $dirs; do
  if [[ -d "\$dir" ]]; then
    count=\$(find "\$dir" -name '*.parquet' 2>/dev/null | wc -l)
    size=\$(du -sh "\$dir" 2>/dev/null | cut -f1)
    printf "    %-36s  %5d files  %s\n" "\$dir" "\$count" "\$size"
  else
    printf "    %-36s  MISSING\n" "\$dir"
  fi
done
REMOTE
}

# Systemd / pgrep service check via SSH
r_systemd() {
  local host=$1 user=$2 key=$3 password=$4
  local services=$5  # space-separated
  [[ -z "$services" ]] && return

  printf "\n  Systemd services:\n"
  ssh_script "$host" "$user" "$key" "$password" << REMOTE
declare -A PROCS=([flashmq]=flashmq [influxdb]=influxd [telegraf]=telegraf
                  [parquet-writer]=parquet_writer [subscriber-api]=python3)
for svc in $services; do
  proc="\${PROCS[\$svc]:-\$svc}"
  if systemctl is-active --quiet "\$svc" 2>/dev/null; then
    printf "    \033[0;32m✓\033[0m  %-20s  active (systemd)\n" "\$svc"
  elif pgrep -x "\$proc" &>/dev/null; then
    printf "    \033[0;32m✓\033[0m  %-20s  running (pgrep)\n" "\$svc"
  else
    printf "    \033[0;31m✗\033[0m  %-20s  inactive\n" "\$svc"
  fi
done
REMOTE
}

# ── per-type check sets ───────────────────────────────────────────────────────

check_edge() {
  local host=$1 user=$2 key=$3 password=$4 parquet_dirs=$5

  r_tcp      "$host" "$user" "$key" "$password" 1883 "FlashMQ host broker"
  r_http     "$host" "$user" "$key" "$password" \
             "http://localhost:8770/ping" "push_agent /ping"
  r_http_json "$host" "$user" "$key" "$password" \
             "http://localhost:8770/rsync/status" "push_agent rsync" \
             "import json,sys; d=json.load(sys.stdin)
print(f'runs={d[\"run_count\"]}  files={d[\"file_count\"]}  {d[\"total_mb\"]}MB  last={d[\"last_ms\"]}ms')"
  r_http_json "$host" "$user" "$key" "$password" \
             "http://localhost:8770/parquet_stats" "push_agent parquet_stats" \
             "import json,sys; d=json.load(sys.stdin)
print(f'files={d[\"file_count\"]}  size={d[\"total_mb\"]}MB  latest_age={d[\"latest_age_s\"]}s')"

  r_parquet_dirs "$host" "$user" "$key" "$password" "$parquet_dirs"
}

check_aws_sim() {
  local host=$1 user=$2 key=$3 password=$4 parquet_dirs=$5 systemd_svcs=$6

  # Brokers
  r_tcp "$host" "$user" "$key" "$password" 1883 "FlashMQ host broker"
  r_tcp "$host" "$user" "$key" "$password" 1884 "FlashMQ aws-sim broker"

  # Old path
  r_tcp  "$host" "$user" "$key" "$password" 8086 "InfluxDB2"
  r_http "$host" "$user" "$key" "$password" "http://localhost:8086/ping" "InfluxDB2 /ping"

  # New path
  r_tcp  "$host" "$user" "$key" "$password" 8767 "Subscriber API"
  # 426 = Upgrade Required — WS server is alive but rejecting plain HTTP
  r_http "$host" "$user" "$key" "$password" "http://localhost:8767/" "Subscriber API (WS)" "24"

  # On-demand services (warn rather than fail if absent)
  for port_label in "8761:Manager" "8769:Stress Runner" "8765:Generator"; do
    local port=${port_label%%:*} label=${port_label##*:}
    if ssh_run "$host" "$user" "$key" "$password" "nc -z -w3 localhost $port" 2>/dev/null; then
      ok "$label (:$port)"
    else
      warn "$label (:$port) — not running (on-demand)"
    fi
  done

  r_parquet_dirs "$host" "$user" "$key" "$password" "$parquet_dirs"
  r_systemd      "$host" "$user" "$key" "$password" "$systemd_svcs"
}

check_aws() {
  local host=$1 user=$2 key=$3 password=$4
  local region=$5 s3_bucket=$6 s3_prefix=$7 ecs_cluster=$8 ecs_services=$9
  local systemd_svcs=${10:-}

  # Port checks go through SSH — security groups likely block direct access
  r_tcp  "$host" "$user" "$key" "$password" 8767 "Subscriber API"
  r_http "$host" "$user" "$key" "$password" "http://localhost:8767/" "Subscriber API (WS)" "24"

  # EC2 instance identity (IMDSv2)
  local instance_id
  instance_id=$(ssh_run "$host" "$user" "$key" "$password" \
    "TOKEN=\$(curl -sf -X PUT 'http://169.254.169.254/latest/api/token' \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 10' 2>/dev/null) && \
     curl -sf -H \"X-aws-ec2-metadata-token: \$TOKEN\" \
       http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo 'N/A'" \
    2>/dev/null || echo "N/A")
  info "EC2 instance: $instance_id"

  # S3 parquet file count
  if [[ -n "$s3_bucket" ]]; then
    printf "\n  S3 parquet data:\n"
    ssh_script "$host" "$user" "$key" "$password" << REMOTE
bucket="$s3_bucket"
prefix="$s3_prefix"
region="${region:-us-east-1}"
count=\$(aws s3 ls "s3://\$bucket/\$prefix" --recursive --region "\$region" 2>/dev/null \
  | grep '\.parquet\$' | wc -l || echo '?')
newest=\$(aws s3 ls "s3://\$bucket/\$prefix" --recursive --region "\$region" 2>/dev/null \
  | grep '\.parquet\$' | sort | tail -1 | awk '{print \$1, \$2}' || echo 'unknown')
printf "    s3://%s/%s\n    files: %s   newest: %s\n" "\$bucket" "\$prefix" "\$count" "\$newest"
REMOTE
  fi

  # ECS service health
  if [[ -n "$ecs_cluster" && -n "$ecs_services" ]]; then
    printf "\n  ECS services:\n"
    ssh_script "$host" "$user" "$key" "$password" << REMOTE
cluster="$ecs_cluster"
region="${region:-us-east-1}"
for svc in $ecs_services; do
  result=\$(aws ecs describe-services \
    --cluster "\$cluster" --services "\$svc" --region "\$region" \
    --query 'services[0].[runningCount,desiredCount,status]' \
    --output text 2>/dev/null || echo "? ? UNKNOWN")
  running=\$(echo "\$result" | awk '{print \$1}')
  desired=\$(echo "\$result" | awk '{print \$2}')
  status=\$(echo "\$result" | awk '{print \$3}')
  if [[ "\$running" == "\$desired" && "\$running" != "?" ]]; then
    printf "    \033[0;32m✓\033[0m  %-24s  %s/%s tasks  (%s)\n" "\$svc" "\$running" "\$desired" "\$status"
  else
    printf "    \033[0;31m✗\033[0m  %-24s  %s/%s tasks  (%s)\n" "\$svc" "\$running" "\$desired" "\$status"
  fi
done
REMOTE
  fi

  # Any systemd services on the EC2 instance itself
  r_systemd "$host" "$user" "$key" "$password" "$systemd_svcs"
}

# ── main loop ─────────────────────────────────────────────────────────────────
for i in $(seq 0 $((SITE_COUNT - 1))); do
  eval "name=\$SITE_${i}_NAME host=\$SITE_${i}_HOST user=\$SITE_${i}_USER"
  eval "type=\$SITE_${i}_TYPE key=\$SITE_${i}_SSH_KEY password=\$SITE_${i}_SSH_PASSWORD"
  eval "parquet_dirs=\$SITE_${i}_PARQUET_DIRS systemd=\$SITE_${i}_SYSTEMD"
  eval "region=\$SITE_${i}_AWS_REGION s3_bucket=\$SITE_${i}_S3_PARQUET_BUCKET"
  eval "s3_prefix=\$SITE_${i}_S3_PARQUET_PREFIX ecs_cluster=\$SITE_${i}_ECS_CLUSTER"
  eval "ecs_services=\$SITE_${i}_ECS_SERVICES"

  hdr "$name  [$host]  type:$type"

  case "$type" in
    edge)
      check_edge "$host" "$user" "$key" "$password" "$parquet_dirs"
      ;;
    aws-sim)
      check_aws_sim "$host" "$user" "$key" "$password" "$parquet_dirs" "$systemd"
      ;;
    aws)
      check_aws "$host" "$user" "$key" "$password" \
        "$region" "$s3_bucket" "$s3_prefix" "$ecs_cluster" "$ecs_services" "$systemd"
      ;;
    *)
      warn "Unknown site type: $type — skipping"
      ;;
  esac
done

printf "\n${BOLD}Done.${RESET}\n\n"
