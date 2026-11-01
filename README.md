# SmartScale AI: Event-Driven Autoscaling Inference Engine

An asynchronous AI inference orchestration system for Kubernetes. Client job ingestion is decoupled from inference execution using **Redis Streams**, job lifecycles are tracked in **Redis Hashes**, and worker pods are scaled from **0 to N** by **KEDA** based on real-time queue depth.

## Architecture

```
                      +-------------------+
                      |   Client / API    |
                      +---------+---------+
                                |
                    POST /api/v1/jobs (XADD)
                                |
                                v
                    +-----------------------+
                    |  Redis Streams Queue  |
                    |    (inference:jobs)   |
                    +---+---------------+---+
                        |               |
           Stream Depth | (XLEN)        | Reads via Consumer Group
                        v               v
                +---------------+   +-----------------------+
                |     KEDA      |   |  Spring Boot Workers  |
                | ScaledObject  |   | (smartscale-worker:v2)|
                +-------+-------+   +-----------+-----------+
                        |                       |
            HPA Replicas|Trigger                | Inference Mock / Compute
                        v                       v
            +-----------------------+   +-----------------------+
            |  Kubernetes Pod Pool  |   |   Redis Job Hashes    |
            |   (Scale 0 <-> 5)     |   |      (job:<id>)       |
            +-----------------------+   +-----------------------+
```

## Core Workflow

1. **Decoupled ingestion**: Tasks are appended to the `inference:jobs` stream with `XADD`. Metadata and status (`QUEUED`) are initialized in a Redis Hash at `job:<jobId>`.
2. **Event-driven autoscaling**: A KEDA `ScaledObject` monitors the stream length (`XLEN`) through the `redis-streams` trigger. When the queue is empty, workers scale down to 0 replicas, so there is no idle resource usage.
3. **Consumer group processing**: Worker pods join the `inference-workers` consumer group and read with `XREADGROUP`.
4. **Stream compaction & scale-to-zero**: After a task finishes, the worker calls:
   - `XACK` to acknowledge the entry in the consumer group
   - `XDEL` to remove the entry from the stream

   This decrements `XLEN`, letting KEDA see an empty queue and scale back to 0 after the cooldown period.
5. **State tracking**: Job state (`PROCESSING` → `COMPLETED` / `FAILED`) and inference output are written back to `job:<jobId>`.

## Tech Stack

| Area | Technology |
|------|------------|
| Language & framework | Java 21, Spring Boot 3.x |
| Data access & client | Spring Data Redis, Lettuce (connection pooling, keep-alives) |
| Queue & state store | Redis Streams, Redis Hashes |
| Cluster & autoscaling | Kubernetes (Minikube), KEDA 2.13+, Horizontal Pod Autoscaler (HPA) |
| Packaging | Docker (multi-stage build) |

## Project Structure

```
├── src/
│   └── main/
│       ├── java/com/example/smartscaleai/
│       │   ├── config/        # Redis template & connection pool configuration
│       │   ├── consumer/      # Stream consumer loop & lifecycle management
│       │   ├── model/         # Job request and state DTOs
│       │   └── SmartscaleaiApp.java
│       └── resources/
│           └── application.yml  # Lettuce timeouts, thread pool & Redis endpoints
├── k8s-manifest.yml           # Redis, API, Worker Deployments, Services & ScaledObject
├── Dockerfile                 # Container build definition
├── pom.xml                    # Maven dependencies
└── README.md
```

## Kubernetes Configuration

### KEDA ScaledObject (`k8s-manifest.yml`)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker-scaler
  namespace: default
spec:
  scaleTargetRef:
    name: smartscale-worker
  minReplicaCount: 0
  maxReplicaCount: 5
  cooldownPeriod: 30
  pollingInterval: 5
  triggers:
    - type: redis-streams
      metadata:
        address: redis-service:6379
        stream: inference:jobs
        streamLength: "2"   # scale on XLEN: roughly 1 pod per 2 queued jobs
```

> **Why `streamLength` and no `consumerGroup`?** With `consumerGroup` set, the KEDA `redis-streams` scaler counts *pending* entries (delivered but not yet acknowledged). At 0 replicas nothing is ever delivered, so it would never scale up from zero. `streamLength` scales on `XLEN`, which matches the `XACK` + `XDEL` compaction design.

### Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smartscale-worker
  namespace: default
spec:
  selector:
    matchLabels:
      app: smartscale-worker
  template:
    metadata:
      labels:
        app: smartscale-worker
    spec:
      containers:
        - name: worker
          image: smartscale-worker:v2
          imagePullPolicy: IfNotPresent
          env:
            - name: SPRING_DATA_REDIS_HOST
              value: "redis-service"
            - name: SPRING_DATA_REDIS_PORT
              value: "6379"
```

## Local Setup & Deployment

### 1. Prerequisites

- Docker Desktop and Minikube
- `kubectl` configured for your Minikube context
- KEDA installed on the cluster:

```powershell
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.13.0/keda-2.13.0.yaml
```

### 2. Build and Load the Image

```powershell
# 1. Package the Spring Boot application
mvn clean package -DskipTests

# 2. Build the Docker image
docker build -t smartscale-worker:v2 .

# 3. Load the image into Minikube
minikube image load smartscale-worker:v2
```

### 3. Deploy Manifests

Deploys Redis, the application workloads and the KEDA autoscaling rules:

```powershell
kubectl apply -f k8s-manifest.yml
```

Verify the initial state (workers should be at 0 replicas):

```powershell
kubectl get pods
```

## Verification & Testing

### 1. Watch scaling in real time

In a dedicated terminal:

```powershell
kubectl get pods -l app=smartscale-worker -w
```

### 2. Push inference workloads

Inject 10 tasks into the stream:

```powershell
1..10 | ForEach-Object {
    kubectl exec deployment/redis -- redis-cli XADD "inference:jobs" "*" jobId "test-$_" prompt "Inference job evaluation $_"
}
```

### 3. Verify queue depletion and results

- **Scale-up**: Within 5–10 seconds KEDA detects `XLEN > 0` and creates pods, up to `maxReplicaCount`.
- **Worker execution**: Workers read from the stream, run the task, then `XACK` and `XDEL` the entry.
- **Stream depletion**: confirm the stream is empty:

  ```powershell
  kubectl exec deployment/redis -- redis-cli XLEN "inference:jobs"
  ```

- **Hash persistence**: confirm task state and response:

  ```powershell
  kubectl exec deployment/redis -- redis-cli HGETALL "job:test-1"
  ```

- **Scale-down**: Once `XLEN` reaches 0, the 30-second cooldown starts. Pods move to `Terminating` and replicas return to 0.

## Current Status & Next Steps

- [x] **Redis Streams & consumer groups**: fully functional, with automatic group creation via `@PostConstruct`
- [x] **Lettuce connection stability**: tuned socket timeouts and connection validation
- [x] **Accurate autoscaling**: atomic stream trimming with `XDEL` on acknowledgment
- [x] **Scale-to-zero verification**: pods scale between 0 and 5 based strictly on queue presence
- [ ] **Real model inference**: replace the mock delay (`Thread.sleep`) with an HTTP/gRPC integration to a local LLM runner (Ollama / vLLM) or a remote inference API
