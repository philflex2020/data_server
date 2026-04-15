# parquet-writer k3s demo — from scratch

Step-by-step guide to running the parquet-writer stack on a fresh machine
using a local k3s cluster.  Tested on Ubuntu 22.04 with Docker installed.

---

## Prerequisites

- Ubuntu 22.04 (or compatible)
- Docker installed (`docker --version`)
- Repo cloned at `~/work/gen-ai/data_server` (or adjust paths)
- An MQTT broker reachable from this host

---

## 1 — Install k3s

```bash
curl -sfL https://get.k3s.io | sh -
```

Wait ~30 s for the service to start, then verify:

```bash
systemctl is-active k3s        # should print: active
sudo k3s kubectl get nodes     # should show node Ready
```

## 2 — Set up kubectl without sudo

```bash
mkdir -p ~/.kube
sudo k3s kubectl config view --raw > ~/.kube/config
chmod 600 ~/.kube/config

# Make permanent — k3s kubectl defaults to /etc/rancher/k3s/k3s.yaml
# so KUBECONFIG must be set explicitly
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
export KUBECONFIG=~/.kube/config

kubectl get nodes              # should show node Ready without sudo
```

## 3 — Pull latest code

```bash
cd ~/work/gen-ai/data_server
git pull
cd source/parquet_writer
```

All remaining commands run from `source/parquet_writer/`.

## 4 — Clean slate

```bash
make purge
```

Removes compiled binaries, /tmp logs, and parquet files from any existing pod.

## 5 — Create namespace and MQTT secret

```bash
kubectl create namespace data-capture

kubectl create secret generic parquet-writer-secrets \
  --from-literal=MQTT_HOST=<broker-ip> \
  -n data-capture
```

Replace `<broker-ip>` with the IP or hostname of your MQTT broker.
This is the only place the broker address is set — it is never stored in
`site.yaml` or the Docker image.

### Local FlashMQ demo (phil-dev / .46)

FlashMQ runs on this host at `:1883`.  Use the host's LAN IP — **not**
`localhost` — because the writer pod runs inside k3s and cannot reach the
host loopback:

```bash
kubectl create secret generic parquet-writer-secrets \
  --from-literal=MQTT_HOST=192.168.86.46 \
  -n data-capture
```

`test_multi_publish.py` runs directly on the host so it can use `localhost`
or the hostname.  The pod uses the LAN IP set in the secret above.

```bash
# publish test data from the host — connects to localhost:1883
export WRITER_HOST=192.168.86.46
python3 test_multi_publish.py --format fractal
```

## 6 — Configure the site

Edit `site.yaml` for this deployment:

| Field | What to set |
|---|---|
| `mqtt.host` | MQTT broker IP (reference only — real value is in the secret above) |
| `mqtt.topic_root` | Subscribe root, e.g. `ems` (writer subscribes to `<root>/#`) |
| `mqtt.client_id` | Unique string per pod, e.g. `parquet-writer-site-a` |
| `mqtt.port` | Broker port (default `1883`) |
| `eks.account_id` | Set to `local` for k3s (no ECR) |
| `eks.namespace` | `data-capture` (must match step 5) |

All other fields can stay at their defaults for a demo.

## 7 — Generate manifests and deploy

The writer binary is compiled **inside the Docker image** (two-stage build)
so no local C++ toolchain is needed.

```bash
make configmap
```

Generates `manifests/` from `site.yaml` and applies the ConfigMap to k3s.
First run may show a rollout error if the pod doesn't exist yet — ignore it.

```bash
make demo
```

Builds `parquet-writer:latest` via Docker (compiles the binary inside),
imports the image into k3s, and bounces the pod.

> `make all` is only needed if you want to run the binary directly on the
> host outside of Docker (e.g. native performance testing). Skip it for
> the standard k3s demo.

## 10 — Verify

```bash
make health                        # curl /health and pretty-print
python3 show_health.py             # same, richer output
```

Expected output includes `status: ok`, `msgs_received` climbing if the
broker is publishing, and `seen_topics` listing matched topic patterns.

---

## Optional — query pod (DuckDB)

To deploy the DuckDB HTTP query server alongside the writer, add to `site.yaml`:

```yaml
eks:
  query_port: 8772
  query_repo: parquet-query   # local image name for k3s
```

Then build and deploy:

```bash
make query-image    # builds parquet-query:latest
make configmap      # regenerates manifests including query-deployment.yaml
make query-demo     # imports image to k3s, bounces pod
```

Query the data:

```bash
export WRITER_HOST=localhost

python3 run_query.py health
python3 run_query.py sites
python3 run_query.py latest --site SITE_A --n 50
python3 run_query.py sql "SELECT point_name, avg(val) FROM data GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

---

## Useful commands

```bash
# Pod status
kubectl get pods -n data-capture

# Writer logs (live)
kubectl logs -f deployment/parquet-writer-long -n data-capture

# Query pod logs
kubectl logs -f deployment/parquet-query -n data-capture

# Restart writer without rebuilding image
kubectl rollout restart deployment/parquet-writer-long -n data-capture

# Wipe parquet data in pod
kubectl exec -n data-capture deployment/parquet-writer-long -- \
  sh -c 'find /data -name "*.parquet" -delete'

# Check k3s node
kubectl get nodes -o wide

# Check storage
kubectl get pvc -n data-capture
```

## Troubleshooting

| Symptom | Check |
|---|---|
| Pod stuck in `Pending` | `kubectl describe pod -n data-capture` — usually PVC or image not found |
| Pod `CrashLoopBackOff` | `kubectl logs -n data-capture <pod>` — likely config or secret missing |
| `make demo` fails on import | Run manual steps from `make help` (sudo password issue) |
| No `seen_topics` in health | Check broker IP in secret; check `mqtt.topic_root` matches broker |
| Query pod returns empty | Writer may not have flushed yet — default flush is 30 s |
