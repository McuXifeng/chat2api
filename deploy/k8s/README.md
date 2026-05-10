# Deploying chat2api on a k3s node

These manifests deploy `chat2api` (gateway) + `redis` + `worker` (Playwright browser
automation) into a k3s cluster running on a single node. Images are built locally
and imported into k3s' containerd; nothing is pulled from a registry.

Layout:

| File | What it deploys |
|---|---|
| `00-namespace.yaml`        | Namespace `chat2api` |
| `10-secret.yaml.example`   | Template for `chat2api-secrets` (DO NOT COMMIT the filled copy) |
| `20-redis.yaml`            | Redis 7 (Streams + InstancePool backend) + 2Gi PVC |
| `30-chat2api.yaml`         | FastAPI gateway, NodePort `30005` → 5005 |
| `40-worker.yaml`           | Playwright worker, 1 replica × 2 slots |

## Prereqs on the node

- k3s ≥ v1.30 with `local-path` storage class (default)
- containerd reachable at `/run/k3s/containerd/containerd.sock`
- Docker installed *only* for building images (the dockerd is unused at runtime;
  see `bash -lc 'apt install -y docker.io'`)
- `ctr` (provided by k3s) on `$PATH`

## Build & import images

From the repo root, on the node:

```bash
TAG=$(git rev-parse --short HEAD)

docker build -t chat2api:$TAG -f Dockerfile .
docker build -t chat2api-worker:$TAG -f workers/Dockerfile .

# Re-tag :current so manifests don't change between deploys
docker tag chat2api:$TAG chat2api:current
docker tag chat2api-worker:$TAG chat2api-worker:current

# Import into k3s' containerd (namespace k3s.io)
docker save chat2api:current        | sudo /usr/local/bin/ctr -n k3s.io images import -
docker save chat2api-worker:current | sudo /usr/local/bin/ctr -n k3s.io images import -
```

## Apply

```bash
# 1) Namespace + secret
kubectl apply -f deploy/k8s/00-namespace.yaml

# 2) Generate the cookie key, fill secret.yaml, apply (NOT committed)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
cp deploy/k8s/10-secret.yaml.example deploy/k8s/secret.yaml
$EDITOR deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/secret.yaml

# 3) Workloads
kubectl apply -f deploy/k8s/20-redis.yaml
kubectl apply -f deploy/k8s/30-chat2api.yaml
kubectl apply -f deploy/k8s/40-worker.yaml

# 4) Watch
kubectl -n chat2api get pods -w
```

The gateway will be reachable at `http://<node-ip>:30005`.

## Updating

After pulling new code:

```bash
TAG=$(git rev-parse --short HEAD)
docker build -t chat2api:current        -f Dockerfile .
docker build -t chat2api-worker:current -f workers/Dockerfile .
docker save chat2api:current        | sudo ctr -n k3s.io images import -
docker save chat2api-worker:current | sudo ctr -n k3s.io images import -
kubectl -n chat2api rollout restart deploy/chat2api deploy/worker
```

## Notes / known gaps

- **Headless = true by default.** Stealth detection on chatgpt.com works better
  with a real X server. Wrap the worker CMD in `xvfb-run` (or run a lightweight
  X server sidecar) once the basic pipeline is verified.
- **`SYS_ADMIN` is granted to the worker** so chromium's user-namespace sandbox
  can run inside a container.  This is the same permission the docker-compose
  setup grants via `cap_add: SYS_ADMIN`.
- `COOKIE_ENCRYPTION_KEY` *must* match between the gateway and worker
  deployments — both read it from the same Secret to ensure that.
