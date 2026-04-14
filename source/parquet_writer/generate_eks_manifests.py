#!/usr/bin/env python3
"""
generate_eks_manifests.py — generate Kubernetes YAMLs from a site descriptor

Usage:
  python3 generate_eks_manifests.py site.yaml [output_dir]

Writes to output_dir (default: ./manifests/):
  pvc.yaml          PersistentVolumeClaim
  configmap.yaml    writer config with MQTT_HOST / SITE_ID placeholders
  deployment.yaml   Deployment referencing the ConfigMap and Secret
  kustomization.yaml  optional — lists all resources in apply order

Apply with:
  kubectl apply -f manifests/ -n <namespace>

topic_format values (set in site.yaml under mqtt:):
  bench   — single flat topic tree, topic_segments + drop_columns (default)
  fractal — Fractal EMS variable-depth topics, all 4 patterns auto-generated
"""
import sys, os, pathlib, subprocess, json
try:
    import yaml
except ImportError:
    print("pip3 install pyyaml"); sys.exit(1)

# Fractal EMS topic patterns (4 depths, first match wins).
# Generated automatically when mqtt.topic_format = fractal.
# Source of truth: config.longbow.yaml
FRACTAL_TOPIC_PATTERNS = """\
      topic_patterns:

        # 9-seg: unit signals with device + instance  (95% of traffic)
        # ems/site/{site}/unit/{unit}/{device}/{instance}/{point}/{dtype}
        - match: "ems/site/+/unit/+/+/+/+/+"
          segments: ["_","_","site_id","_","unit_id","device","instance","point_name","dtype_hint"]

        # 8-seg: unit root signals — no instance level
        # ems/site/{site}/unit/{unit}/root/{point}/{dtype}
        - match: "ems/site/+/unit/+/+/+/+"
          segments: ["_","_","site_id","_","unit_id","device","point_name","dtype_hint"]

        # 7-seg: meter/rtac/site-device signals — no unit
        # ems/site/{site}/{device}/{instance}/{point}/{dtype}
        - match: "ems/site/+/+/+/+/+"
          segments: ["_","_","site_id","device","instance","point_name","dtype_hint"]

        # 6-seg: site-level root signals — no unit or instance
        # ems/site/{site}/root/{point}/{dtype}
        - match: "ems/site/+/+/+/+"
          segments: ["_","_","site_id","device","point_name","dtype_hint"]"""


def load(path):
    with open(path) as f:
        return yaml.safe_load(f)

def resolve_image(eks):
    """Build full ECR image URI, resolving account_id='auto' via AWS CLI if needed."""
    account_id = str(eks.get("account_id", ""))
    region     = eks["region"]
    repo       = eks["repo"]
    tag        = eks.get("tag", "latest")

    if account_id.lower() == "auto":
        try:
            out = subprocess.check_output(
                ["aws", "sts", "get-caller-identity", "--output", "json"],
                stderr=subprocess.DEVNULL
            )
            account_id = json.loads(out)["Account"]
            print(f"  resolved account_id: {account_id}  (via aws sts get-caller-identity)")
        except Exception as e:
            print(f"ERROR: account_id is 'auto' but aws sts get-caller-identity failed: {e}")
            print("       Either run `aws configure` or set account_id explicitly in site.yaml")
            sys.exit(1)

    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{repo}:{tag}"

def write(path, text):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
    print(f"  wrote {path}")

def pvc_yaml(s):
    eks = s["eks"]
    return f"""apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: parquet-capture-pvc
  namespace: {eks["namespace"]}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: {eks["storage_class"]}
  resources:
    requests:
      storage: {eks["storage_gb"]}Gi
"""

def _mqtt_block_bench(mqtt):
    """topic_segments / drop_columns style (bench / simple flat topics)."""
    return f"""\
      topic:        {mqtt["topic_root"]}/#
      topic_parser: positional
      topic_segments: {mqtt["topic_segments"]}
      drop_columns:   {mqtt["drop_columns"]}
      partition_field: {mqtt["partition_field"]}"""

def _mqtt_block_fractal(mqtt):
    """Variable-depth topic_patterns style (Fractal EMS)."""
    return f"""\
      topic:        ems/#
      topic_parser: positional
      partition_field: site_id
{FRACTAL_TOPIC_PATTERNS}"""

def _output_block_bench(cap, wide):
    """Output block for bench / simple flat topic format."""
    return f"""\
      base_path:    /data/site-capture
      partitions:   ['{{year}}', '{{month}}', '{{day}}']
      partition_as_filename_prefix: true
      wide_point_name: {wide}
      flush_interval_seconds: {cap["flush_interval_seconds"]}
      compression:  {cap["compression"]}
      store_mqtt_topic:   false
      store_sample_count: false
      site_id:      SITE_ID_PLACEHOLDER
      max_messages_per_part: {cap["max_messages_per_part"]}
      max_total_buffer_rows: {cap["max_total_buffer_rows"]}"""

def _output_block_fractal(cap):
    """Output block for Fractal format — site_id comes from topic, not secret."""
    return f"""\
      base_path:    /data/site-capture
      site_id:      ""
      partitions:   ['{{site_id}}', '{{year}}', '{{month}}', '{{day}}']
      flush_interval_seconds: {cap["flush_interval_seconds"]}
      compression:  {cap["compression"]}
      store_mqtt_topic:   false
      store_sample_count: false
      max_messages_per_part: {cap["max_messages_per_part"]}
      max_total_buffer_rows: {cap["max_total_buffer_rows"]}"""

def configmap_yaml(s):
    eks  = s["eks"]
    mqtt = s["mqtt"]
    cap  = s["capture"]
    fmt  = cap["format"]
    topic_fmt = mqtt.get("topic_format", "bench")
    wide = "true" if fmt == "wide" else "false"

    if topic_fmt == "fractal":
        mqtt_block   = _mqtt_block_fractal(mqtt)
        output_block = _output_block_fractal(cap)
    else:
        mqtt_block   = _mqtt_block_bench(mqtt)
        output_block = _output_block_bench(cap, wide)

    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: parquet-writer-config
  namespace: {eks["namespace"]}
data:
  config.yaml: |
    mqtt:
      host:         MQTT_HOST_PLACEHOLDER
      port:         {mqtt["port"]}
      client_id:    {mqtt["client_id"]}
      qos:          {mqtt["qos"]}
{mqtt_block}
    output:
{output_block}
    compact:
      enabled: {str(cap["compact_enabled"]).lower()}
    guard:
      min_free_gb: 1.0
    health:
      enabled: true
      port: {eks["health_port"]}
    wal:
      enabled: true
"""

def deployment_yaml(s, image):
    eks      = s["eks"]
    site     = s["site"]
    mqtt     = s["mqtt"]
    port     = eks["health_port"]
    delay    = eks["liveness_initial_delay_seconds"]
    topic_fmt = mqtt.get("topic_format", "bench")
    unit_label = site.get("unit_id", "n-a")

    # For fractal format, SITE_ID comes from the MQTT topic — the secret only
    # carries MQTT_HOST.  We still inject SITE_ID for parity; entrypoint.sh will
    # only replace the placeholder if it exists in the config, so it's harmless.
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: {eks["deployment_name"]}
  namespace: {eks["namespace"]}
  labels:
    site: {site["id"]}
    unit: {unit_label}
    topic-format: {topic_fmt}
spec:
  strategy:
    type: Recreate
  replicas: 1
  selector:
    matchLabels:
      app: parquet-writer
  template:
    metadata:
      labels:
        app: parquet-writer
        site: {site["id"]}
        unit: {unit_label}
    spec:
      containers:
        - name: writer
          image: {image}
          command: ["/entrypoint.sh"]
          env:
            - name: MQTT_HOST
              valueFrom:
                secretKeyRef:
                  name: parquet-writer-secrets
                  key: MQTT_HOST
            - name: SITE_ID
              valueFrom:
                secretKeyRef:
                  name: parquet-writer-secrets
                  key: SITE_ID
          volumeMounts:
            - name: config
              mountPath: /etc/writer
              readOnly: true
            - name: data
              mountPath: /data
          ports:
            - containerPort: {port}
              hostPort: {port}
          livenessProbe:
            httpGet:
              path: /health
              port: {port}
            initialDelaySeconds: {delay}
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health
              port: {port}
            initialDelaySeconds: {delay}
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: parquet-writer-config
        - name: data
          persistentVolumeClaim:
            claimName: parquet-capture-pvc
"""

def kustomization_yaml(s):
    return """apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - pvc.yaml
  - configmap.yaml
  - deployment.yaml
"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 generate_eks_manifests.py site.yaml [output_dir]")
        sys.exit(1)

    site_file  = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "manifests"
    s = load(site_file)

    topic_fmt = s["mqtt"].get("topic_format", "bench")
    image = resolve_image(s["eks"])
    print(f"Generating manifests for site={s['site']['id']}  topic_format={topic_fmt}")
    print(f"  image: {image}")
    write(f"{output_dir}/pvc.yaml",           pvc_yaml(s))
    write(f"{output_dir}/configmap.yaml",     configmap_yaml(s))
    write(f"{output_dir}/deployment.yaml",    deployment_yaml(s, image))
    write(f"{output_dir}/kustomization.yaml", kustomization_yaml(s))
    ns     = s["eks"]["namespace"]
    host   = s["mqtt"]["host"]
    sid    = s["site"]["id"]
    region = s["eks"]["region"]

    print(f"\nPush image to ECR:")
    print(f"  aws ecr get-login-password --region {region} | \\")
    print(f"    docker login --username AWS --password-stdin {image.split('/')[0]}")
    print(f"  docker tag parquet-writer:latest {image}")
    print(f"  docker push {image}")
    print(f"\nCreate namespace + secret:")
    print(f"  kubectl create namespace {ns}")
    if topic_fmt == "fractal":
        print(f"  # For fractal format, SITE_ID is derived from the MQTT topic.")
        print(f"  # Set SITE_ID to a placeholder value (e.g. the site name).")
    print(f"  kubectl create secret generic parquet-writer-secrets \\")
    print(f"    --from-literal=MQTT_HOST={host} \\")
    print(f"    --from-literal=SITE_ID={sid} \\")
    print(f"    -n {ns}")
    print(f"\nApply manifests:")
    print(f"  kubectl apply -f {output_dir}/ -n {ns}")
