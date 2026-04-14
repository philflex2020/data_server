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
"""
import sys, os, pathlib, subprocess, json
try:
    import yaml
except ImportError:
    print("pip3 install pyyaml"); sys.exit(1)

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

def configmap_yaml(s):
    eks  = s["eks"]
    mqtt = s["mqtt"]
    cap  = s["capture"]
    fmt  = cap["format"]
    wide = "true" if fmt == "wide" else "false"
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
      topic:        {mqtt["topic_root"]}/#
      client_id:    {mqtt["client_id"]}
      qos:          {mqtt["qos"]}
      topic_parser: positional
      topic_segments: {mqtt["topic_segments"]}
      drop_columns:   {mqtt["drop_columns"]}
      partition_field: {mqtt["partition_field"]}
    output:
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
      max_total_buffer_rows: {cap["max_total_buffer_rows"]}
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
    eks  = s["eks"]
    site = s["site"]
    port = eks["health_port"]
    delay = eks["liveness_initial_delay_seconds"]
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: {eks["deployment_name"]}
  namespace: {eks["namespace"]}
  labels:
    site: {site["id"]}
    unit: {site["unit_id"]}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: parquet-writer
  template:
    metadata:
      labels:
        app: parquet-writer
        site: {site["id"]}
        unit: {site["unit_id"]}
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

    image = resolve_image(s["eks"])
    print(f"Generating manifests for site={s['site']['id']} unit={s['site']['unit_id']}")
    print(f"  image: {image}")
    write(f"{output_dir}/pvc.yaml",           pvc_yaml(s))
    write(f"{output_dir}/configmap.yaml",     configmap_yaml(s))
    write(f"{output_dir}/deployment.yaml",    deployment_yaml(s, image))
    write(f"{output_dir}/kustomization.yaml", kustomization_yaml(s))
    ns   = s["eks"]["namespace"]
    host = s["mqtt"]["host"]
    sid  = s["site"]["id"]
    region = s["eks"]["region"]

    print(f"\nPush image to ECR:")
    print(f"  aws ecr get-login-password --region {region} | \\")
    print(f"    docker login --username AWS --password-stdin {image.split('/')[0]}")
    print(f"  docker tag parquet-writer:latest {image}")
    print(f"  docker push {image}")
    print(f"\nCreate namespace + secret:")
    print(f"  kubectl create namespace {ns}")
    print(f"  kubectl create secret generic parquet-writer-secrets \\")
    print(f"    --from-literal=MQTT_HOST={host} \\")
    print(f"    --from-literal=SITE_ID={sid} \\")
    print(f"    -n {ns}")
    print(f"\nApply manifests:")
    print(f"  kubectl apply -f {output_dir}/ -n {ns}")
