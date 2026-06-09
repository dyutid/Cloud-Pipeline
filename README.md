# Radar Pipeline — Complete Technical Documentation
## Automation Edition

> **Scope:** This document covers the full radar pipeline for multi-hub deployments.
> For first-time AWS/Greengrass setup, see `SETUP_NEW_USER.md`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Prerequisites](#3-prerequisites)
4. [Configuration — hubs.yaml](#4-configuration--hubsyaml)
5. [Onboarding a New Hub — provision_hub.py](#5-onboarding-a-new-hub--provision_hubpy)
6. [Starting the Pipeline — start_pipeline.py](#6-starting-the-pipeline--start_pipelinepy)
7. [Deploying Updates — deploy_all.py](#7-deploying-updates--deploy_allpy)
8. [Health Checks — health_check_all.py](#8-health-checks--health_check_allpy)
9. [Occupancy Pipeline — Deep Dive](#9-occupancy-pipeline--deep-dive)
10. [Fall Detection Pipeline — Deep Dive](#10-fall-detection-pipeline--deep-dive)
11. [Wiring in the Fall Detection Model](#11-wiring-in-the-fall-detection-model)
12. [Stopping the Pipeline](#12-stopping-the-pipeline)
13. [Troubleshooting](#13-troubleshooting)
14. [Quick Reference](#14-quick-reference)

---

## 1. Architecture Overview

The system runs two parallel pipelines from each Jetson hub. Both pipelines
originate from `main.py` running on the Jetson and terminate in AWS services.

### 1.1 Occupancy Pipeline

```text
Jetson (main.py)
    │
    │  protobuf over TCP (port 9000)
    ▼
com.example.RadarPublisher   ← Greengrass component on Jetson
    │
    │  AWS Stream Manager
    ▼
Amazon Kinesis (radar-stream)
    │
    │  triggers
    ▼
AWS Lambda (radar-transform)
    ├──▶  Amazon S3          radar-data-landing-/radar-parquet/
    │     (parquet archive)  partitioned by radar_id / date
    │
    └──▶  Amazon RDS PostgreSQL (radardb)
              table: presence_events
                     fixture_assignments
                     current_room_occupancy (view)
                          │
                          │  queries
                          ▼
              API Gateway  →  Dashboard (polls every 30s)
              GET /occupancy?facility_id=fac_001
```

### 1.2 Fall Detection Pipeline

```text
Jetson (main.py)
    │
    │  JSON point clouds over TCP (port 9001)
    ▼
com.example.FallPublisher    ← Greengrass component on Jetson
    │  (runs fall detection model)
    │
    │  MQTT over TLS (port 8883)
    │  topic: triad/falls/
    ▼
AWS IoT Core
    │
    │  IoT Rule: SELECT * FROM 'triad/falls/+'
    ▼
AWS Lambda (triad-fall-handler)
    ├──▶  Amazon DynamoDB (TriadFallEvents)
    └──▶  Amazon SNS  →  email / SMS to caregivers
```

### 1.3 Greengrass Component Map

Every Jetson runs exactly three Greengrass components:

| Component | Version | Role |
|---|---|---|
| `aws.greengrass.StreamManager` | 2.3.0 | Buffers and forwards data from RadarPublisher to Kinesis |
| `com.example.RadarPublisher` | 1.0.0 | Bridge — receives protobuf from main.py on port 9000, forwards to Stream Manager |
| `com.example.FallPublisher` | 1.0.0 | Receives point clouds from main.py on port 9001, runs fall model, publishes to IoT Core |

### 1.4 Network Ports (per Jetson)

| Port | Direction | Used by |
|---|---|---|
| 9000 | main.py → RadarPublisher | Occupancy data (protobuf, gzip) |
| 9001 | main.py → FallPublisher | Point cloud frames (JSON, newline-delimited) |
| 5000 | Radar node → main.py | Radar node 1 raw data |
| 5001 | Radar node → main.py | Radar node 2 raw data |
| 8883 | FallPublisher → IoT Core | MQTT over TLS |

---

## 2. Repository Structure

```text
project-root/
│
├── hubs.yaml                          # All hub definitions — edit this first
│
├── provision_hub.py                   # Run once per new Jetson hub
├── start_pipeline.py                  # Run every session (Steps 1–7, 9)
├── deploy_all.py                      # Redeploy fall_publisher to all hubs
├── health_check_all.py                # End-to-end health check
│
├── fall_publisher.py                  # Fall detection Greengrass component source
│                                      # (deployed to Jetson via provision/deploy)
│
└── Replicating Thesis With OOB DEMO/
    └── MMWave_Radar_Human_Tracking_and_Fall_detection/
        ├── main.py                    # Main radar pipeline (runs on Jetson)
        ├── cfg/
        │   └── config_wireless_2R.py  # Radar + cloud publisher config
        └── library/
            ├── radar.proto            # Protobuf schema
            └── radar_pb2.py           # Generated — regenerate if out of date
```

---

## 3. Prerequisites

### 3.1 Laptop (your machine)

```bash
pip install paramiko boto3 pyyaml psycopg2-binary
```

| Package | Used for |
|---|---|
| `paramiko` | SSH into Jetson hubs |
| `boto3` | All AWS API calls (Greengrass, Kinesis, S3, DynamoDB, SNS, Lambda logs) |
| `pyyaml` | Reading hubs.yaml |
| `psycopg2-binary` | PostgreSQL health check |

AWS credentials must be configured with permissions for:
`greengrassv2`, `iot`, `kinesis`, `s3`, `dynamodb`, `sns`, `logs`, `sts`

```bash
aws configure                   # or use environment variables / IAM role
aws sts get-caller-identity     # verify credentials work
```

### 3.2 Each Jetson Hub (already set up by provision_hub.py)

- Python 3.8+
- `paho-mqtt` (`pip3 install paho-mqtt`)
- Greengrass v2 installed and running
- Certificates provisioned under `/greengrass/v2/`

### 3.3 AWS Resources (already deployed — do not recreate)

| Resource | Name / ARN |
|---|---|
| Kinesis stream | `radar-stream` (us-east-1) |
| Lambda — occupancy | `radar-transform` |
| Lambda — fall detection | `triad-fall-handler` |
| S3 bucket | `radar-data-landing-540321527148` |
| RDS PostgreSQL | `radar-db.csn240sme5cw.us-east-1.rds.amazonaws.com` |
| API Gateway | `https://xto6udtaqd.execute-api.us-east-1.amazonaws.com` |
| DynamoDB table | `TriadFallEvents` |
| IoT Core endpoint | `a1kog7dzc42oa0-ats.iot.us-east-1.amazonaws.com` |
| S3 — Greengrass artifacts | `radar-greengrass-components-540321527148` |
| SNS alert topic | contains `fall` in ARN |

---

## 4. Configuration — hubs.yaml

`hubs.yaml` is the single source of truth for all hubs. Every script reads
it. Edit this file whenever you add, remove, or change a hub.

### 4.1 Full annotated example

```yaml
global:
  aws_region:       "us-east-1"
  s3_bucket:        "radar-greengrass-components-540321527148"
  s3_artifact_key:  "artifacts/fall_publisher.py"
  iot_endpoint:     "a1kog7dzc42oa0-ats.iot.us-east-1.amazonaws.com"
  account_id:       "540321527148"

  # SSH key used for all hubs unless overridden per-hub.
  # Set to null to rely on SSH agent or ~/.ssh/id_rsa.
  default_ssh_key_path: "~/.ssh/id_rsa"

  postgresql:
    host:     "radar-db.csn240sme5cw.us-east-1.rds.amazonaws.com"
    port:     5432
    dbname:   "radardb"
    user:     "radaruser"
    password: "YOUR_DB_PASSWORD"      # ← fill in; never commit to git

  greengrass_components:
    stream_manager:  "2.3.0"
    radar_publisher: "1.0.0"
    fall_publisher:  "1.0.0"          # bump this when you redeploy

hubs:
  - name:             "Jetson-RadarEdge-001"
    ip:               "192.168.1.101"
    ssh_user:         "nvidia"
    thing_name:       "Jetson-RadarEdge-001"   # must match IoT Core thing name
    fall_threshold:   0.7
    point_cloud_port: 9001
    # ssh_key_path: "~/.ssh/hub001_key"        # uncomment to override per hub

  - name:             "Jetson-RadarEdge-002"
    ip:               "192.168.1.102"
    ssh_user:         "nvidia"
    thing_name:       "Jetson-RadarEdge-002"
    fall_threshold:   0.7
    point_cloud_port: 9001
```

### 4.2 Adding a new hub

Append a new block to the `hubs` list:

```yaml
  - name:             "Jetson-RadarEdge-003"
    ip:               "192.168.1.103"
    ssh_user:         "nvidia"
    thing_name:       "Jetson-RadarEdge-003"
    fall_threshold:   0.7
    point_cloud_port: 9001
```

Then run `provision_hub.py` — see Section 5.

### 4.3 SSH key resolution order

1. `ssh_key_path` on the individual hub entry
2. `default_ssh_key_path` in `global`
3. SSH agent / `~/.ssh/id_rsa`

### 4.4 Security note

> ⚠️ Add `hubs.yaml` to `.gitignore`. It contains the PostgreSQL password
> and certificate paths. Never commit it to version control.

---

## 5. Onboarding a New Hub — provision_hub.py

Run once per new Jetson. Automates the entire fall detection setup
(Steps 10.1–10.7 in the original runbook) over SSH + boto3.

### 5.1 What it does, step by step

| Step | Original runbook step | What the script does |
|---|---|---|
| Find certs | 10.1 | `sudo find /greengrass/v2` for thingCert, privKey, rootCA |
| Install paho-mqtt | 10.2 | `pip3 install paho-mqtt` on Jetson; verifies import |
| Test IoT Core | 10.3 | Publishes a test MQTT message; scans DynamoDB to confirm receipt |
| Create fall_publisher.py | 10.4 | Writes source to `~/fall-publisher/artifacts/` on Jetson |
| Create recipe.yaml | 10.5 | Writes recipe with per-hub cert paths and HUB_ID injected |
| Upload + register | 10.6 | SFTPs artifact to laptop, uploads to S3, calls `create_component_version` |
| Deploy + poll | 10.7 | `create_deployment` targeting hub's thing ARN; polls until all three RUNNING |

### 5.2 Usage

```bash
# Standard — provision a new hub:
python3 provision_hub.py --hub Jetson-RadarEdge-003

# Preview without making any changes:
python3 provision_hub.py --hub Jetson-RadarEdge-003 --dry-run

# Skip the IoT connection test (faster, not recommended for first run):
python3 provision_hub.py --hub Jetson-RadarEdge-003 --skip-iot-test

# Specify a custom component version:
python3 provision_hub.py --hub Jetson-RadarEdge-003 --component-version 1.0.1

# Use a different config file:
python3 provision_hub.py --hub Jetson-RadarEdge-003 --config /path/to/hubs.yaml
```

### 5.3 Expected output

```text
============================================================
  Provisioning hub: Jetson-RadarEdge-003  (192.168.1.103)
============================================================

[SSH] Connecting to Jetson...
  [Jetson-RadarEdge-003] ✅  Connected to 192.168.1.103

[Step 10.1] Finding certificate paths on Jetson...
  [Jetson-RadarEdge-003] ✅  cert:    /greengrass/v2/thingCert.pem
  [Jetson-RadarEdge-003] ✅  key:     /greengrass/v2/privKey.pem
  [Jetson-RadarEdge-003] ✅  root_ca: /greengrass/v2/rootCA.pem

[Step 10.2] Installing paho-mqtt on Jetson...
  [Jetson-RadarEdge-003] ✅  paho-mqtt installed and importable

[Step 10.3] Testing IoT Core connection from Jetson...
  [Jetson-RadarEdge-003] ℹ️   Verifying test event landed in DynamoDB...
  [Jetson-RadarEdge-003] ✅  Test event confirmed in DynamoDB

[Step 10.4] Creating fall_publisher.py on Jetson...
  [Jetson-RadarEdge-003] ✅  fall_publisher.py written

[Step 10.5] Creating recipe.yaml on Jetson...
  [Jetson-RadarEdge-003] ✅  recipe.yaml written (component v1.0.0)

[Step 10.6] Uploading artifact to S3 and registering component...
  ✅  Uploaded fall_publisher.py → s3://radar-greengrass-components-540321527148/artifacts/fall_publisher.py
  ✅  Component registered: com.example.FallPublisher v1.0.0

[Step 10.7] Deploying all three components to Jetson...
  ✅  Deployment created — polling for RUNNING state...
  ✅  All three components RUNNING ✓
       aws.greengrass.StreamManager  →  RUNNING
       com.example.FallPublisher     →  RUNNING
       com.example.RadarPublisher    →  RUNNING

============================================================
  ✅  Hub Jetson-RadarEdge-003 provisioned successfully!
  Next: run health_check_all.py to confirm end-to-end
============================================================
```

### 5.4 If provision_hub.py fails mid-way

The script is safe to re-run. Each step checks current state before acting:

- If the component version already exists in Greengrass, it skips
  registration and proceeds to deployment.
- If `paho-mqtt` is already installed, pip is a no-op.
- If certs are already found, it reuses them.

Re-run the same command; it will pick up from where it can.

---

## 6. Starting the Pipeline — start_pipeline.py

Run at the start of every session. Replaces manual Steps 1–7 from the
original runbook. Operates on all hubs in parallel by default.

### 6.1 What it does per hub

| Step | What it checks / does |
|---|---|
| 1 — Greengrass | `systemctl is-active greengrass`; starts it if inactive; waits 10s and re-checks |
| 2 — Components | Calls `greengrassv2.list_installed_components` via boto3; waits up to 60s for RUNNING |
| 3 — Bridge heartbeat | Greps `/greengrass/v2/logs/com.example.RadarPublisher.log` for heartbeat line; waits up to 35s |
| 4 — Port cleanup | `lsof` on ports 5000 and 5001; `kill -9` any process holding them |
| 5 — Config check | Greps `config_wireless_2R.py` for `enable: True` in CLOUD_PUBLISHER_CFG |
| 6 — Radar nodes | Prints reminder — this step is physical and cannot be automated |
| 7 — Start main.py | `nohup python3 main.py` detached; verifies PID; checks log for CloudPublisher connected |

### 6.2 Usage

```bash
# Start all hubs in parallel:
python3 start_pipeline.py

# Start a single hub:
python3 start_pipeline.py --hub Jetson-RadarEdge-001

# Preview without executing:
python3 start_pipeline.py --dry-run

# Stop all hubs cleanly (Step 9):
python3 start_pipeline.py --stop

# Stop a single hub:
python3 start_pipeline.py --stop --hub Jetson-RadarEdge-001

# Control parallelism:
python3 start_pipeline.py --workers 3
```

### 6.3 Expected output

```text
=================================================================
  Starting radar pipeline on 2 hub(s)
=================================================================

  [Jetson-RadarEdge-001          ] ✅  SSH connected (192.168.1.101)
  [Jetson-RadarEdge-001          ] ℹ️   Step 1 — checking Greengrass...
  [Jetson-RadarEdge-001          ] ✅  Greengrass already active
  [Jetson-RadarEdge-001          ] ℹ️   Step 2 — checking Greengrass components...
  [Jetson-RadarEdge-001          ] ✅  All components RUNNING
  [Jetson-RadarEdge-001          ] ℹ️   Step 3 — checking bridge heartbeat...
  [Jetson-RadarEdge-001          ] ✅  Bridge is heartbeating ✓
  [Jetson-RadarEdge-001          ] ℹ️   Step 4 — clearing ports 5000/5001...
  [Jetson-RadarEdge-001          ] ✅  Port 5000 already free
  [Jetson-RadarEdge-001          ] ✅  Port 5001 already free
  [Jetson-RadarEdge-001          ] ℹ️   Step 5 — verifying radar config...
  [Jetson-RadarEdge-001          ] ✅  Cloud publisher enabled in config ✓
  [Jetson-RadarEdge-001          ] ℹ️   Step 6 — ensure radar nodes are powered on (manual)
  [Jetson-RadarEdge-001          ] ℹ️   Step 7 — starting main.py...
  [Jetson-RadarEdge-001          ] ✅  main.py running (PID 14732)
  [Jetson-RadarEdge-001          ] ✅  CloudPublisher connected to bridge ✓

=================================================================
  HUB                                STATUS
  ---                                ------
  Jetson-RadarEdge-001               ✅  ok
  Jetson-RadarEdge-002               ✅  ok
=================================================================

  ✅  Done. Run health_check_all.py to verify end-to-end.
```

### 6.4 main.py log location on Jetson

`start_pipeline.py` starts `main.py` with stdout/stderr redirected to:

```text
/tmp/main_py_.log
```

To inspect it manually:

```bash
ssh nvidia@192.168.1.101 "tail -f /tmp/main_py_Jetson-RadarEdge-001.log"
```

### 6.5 Note on Step 6 — radar nodes

Powering on the ESP32 bridge and radar nodes cannot be scripted — it is
physical hardware. Make sure nodes are advertising on the addresses defined
in `RDR_CFG_LIST` in `config_wireless_2R.py` before running
`start_pipeline.py`. The pipeline will connect on startup and retry forever
if nodes are missing, but you will see no `[DIAG]` lines until they are
reachable.

---

## 7. Deploying Updates — deploy_all.py

Use whenever you update `fall_publisher.py` — for example after wiring in
a new model version or fixing a bug.

### 7.1 What it does

1. Uploads the local `fall_publisher.py` to S3 (once — shared by all hubs)
2. Registers a new component version in Greengrass (once)
3. Calls `create_deployment` targeting each hub's thing ARN (in parallel)
4. Polls each hub until all three components show RUNNING
5. Prints a summary table

No SSH is required — this script is pure boto3.

### 7.2 Usage

```bash
# Deploy updated fall_publisher.py to all hubs:
python3 deploy_all.py --version 1.0.2

# Deploy to a single hub only:
python3 deploy_all.py --version 1.0.2 --hub Jetson-RadarEdge-001

# Use a different artifact path:
python3 deploy_all.py --version 1.0.2 --artifact /path/to/fall_publisher.py

# Preview without making AWS calls:
python3 deploy_all.py --version 1.0.2 --dry-run

# Control parallelism (default 5):
python3 deploy_all.py --version 1.0.2 --workers 10
```

### 7.3 Version numbering convention

| Change type | Example bump |
|---|---|
| Bug fix in fall_publisher.py | 1.0.0 → 1.0.1 |
| New model version wired in | 1.0.1 → 1.1.0 |
| Breaking change to protocol | 1.1.0 → 2.0.0 |

> ⚠️ Greengrass does not allow re-using a version string. Always bump
> `--version` when calling `deploy_all.py`. If you forget, the script will
> warn you that the version already exists and skip registration, but
> deployment will still proceed using the existing artifact.

### 7.4 Expected output

```text
=================================================================
  Deploying fall_publisher v1.0.2 to 2 hub(s)
=================================================================

  ✅  Uploaded fall_publisher.py → s3://radar-greengrass-components.../artifacts/fall_publisher.py
  ✅  Registered com.example.FallPublisher v1.0.2

  Deploying to 2 hub(s) with 5 parallel workers...

  [Jetson-RadarEdge-001          ] ℹ️   Deployment created → polling for RUNNING...
  [Jetson-RadarEdge-002          ] ℹ️   Deployment created → polling for RUNNING...
  [Jetson-RadarEdge-001          ] ✅  All RUNNING (v1.0.2)
  [Jetson-RadarEdge-002          ] ✅  All RUNNING (v1.0.2)

=================================================================
  HUB                                STATUS       DETAIL
  ---                                ------       ------
  Jetson-RadarEdge-001               ✅ ok         1.0.2
  Jetson-RadarEdge-002               ✅ ok         1.0.2
=================================================================

  ✅  All 2 hub(s) deployed successfully.
```

---

## 8. Health Checks — health_check_all.py

Checks every layer of both pipelines in one command. Run after starting a
session or any time you suspect something is wrong.

### 8.1 Checks performed

**Shared (run once, covers the whole AWS pipeline):**

| Check | What it verifies | Tool |
|---|---|---|
| `kinesis` | Records arriving in `radar-stream` | boto3 `get_records` |
| `s3_parquet` | Parquet files written in last 5 min | boto3 `list_objects_v2` |
| `lambda_writes` | `presence_events: wrote` in Lambda logs in last 5 min | boto3 `filter_log_events` |
| `postgresql` | Rows in `presence_events` within last 120s | psycopg2 direct query |
| `api` | API Gateway returns valid rooms/occupied JSON | urllib |
| `sns` | SNS subscription confirmed (not pending) | boto3 `list_subscriptions_by_topic` |

**Per hub (run in parallel for each hub):**

| Check | What it verifies | Tool |
|---|---|---|
| `greengrass` | All three components RUNNING | boto3 `list_installed_components` |
| `fall_events` | DynamoDB events from this hub in last 5 min | boto3 DynamoDB query/scan |

### 8.2 Usage

```bash
# Check all hubs:
python3 health_check_all.py

# Check a single hub:
python3 health_check_all.py --hub Jetson-RadarEdge-001

# Machine-readable JSON output (for monitoring systems):
python3 health_check_all.py --json

# Control parallelism:
python3 health_check_all.py --workers 10
```

### 8.3 Expected healthy output

```text
=================================================================
  Health check — 2 hub(s)  [2024-11-14 14:32:01]
=================================================================

  Shared pipeline checks:
    ✅  kinesis              3 recent record(s), latest at 14:31:58
    ✅  s3_parquet           7 parquet file(s) in last 5min
    ✅  lambda_writes        12 write(s) in last 5min
    ✅  postgresql           5 recent row(s), latest 18s ago
    ✅  api                  rooms=3 occupied=1
    ✅  sns                  2 confirmed subscription(s)

  Hub: Jetson-RadarEdge-001
    ✅  greengrass           all RUNNING
    ✅  fall_events          0 event(s) in last 5min

  Hub: Jetson-RadarEdge-002
    ✅  greengrass           all RUNNING
    ✅  fall_events          0 event(s) in last 5min

=================================================================
  ✅  All 8 checks passed
=================================================================
```

### 8.4 Exit codes

| Code | Meaning |
|---|---|
| `0` | All checks passed |
| `1` | One or more checks failed |

This makes the script suitable for use in monitoring pipelines or CI:

```bash
python3 health_check_all.py || alert "pipeline health check failed"
```

### 8.5 Interpreting fall_events count

`fall_events = 0 event(s) in last 5min` is **normal** when no one has
fallen. It means the pipeline is connected and waiting. A count only
increments when the model fires above threshold. To confirm the fall
pipeline is actually wired end-to-end, trigger a test event manually
(see Section 10.5).

---

## 9. Occupancy Pipeline — Deep Dive

### 9.1 Data flow detail

**main.py → RadarPublisher (port 9000)**

`main.py` serialises each processed radar frame as a protobuf message
(defined in `library/radar.proto`), gzip-compresses it, and sends it over a
TCP socket to `127.0.0.1:9000`. The bridge component (`RadarPublisher`)
accepts the connection and forwards each payload to AWS Stream Manager.

**Stream Manager → Kinesis**

Stream Manager is a Greengrass managed component that provides a reliable
local queue. It batches payloads and forwards them to the `radar-stream`
Kinesis data stream. It handles retries and back-pressure automatically.

**Kinesis → Lambda (radar-transform)**

The `radar-transform` Lambda is triggered by Kinesis. For each record
batch it:

1. Decodes and decompresses the protobuf payload
2. Extracts track-level occupancy features
3. Writes a parquet file to S3 (partitioned by `radar_id` and `date`)
4. Writes a row to `presence_events` in PostgreSQL

**PostgreSQL schema (relevant tables)**

```sql
-- Raw occupancy events from Lambda
presence_events (
    fixture_id    TEXT,       -- maps to a room/zone
    occupied      BOOLEAN,
    avg_snr       FLOAT,
    window_start  TIMESTAMPTZ,
    window_end    TIMESTAMPTZ
)

-- Maps fixture IDs to rooms
fixture_assignments (
    fixture_id    TEXT PRIMARY KEY,
    room_id       TEXT,
    facility_id   TEXT
)

-- View used by API Gateway
current_room_occupancy (
    room_id       TEXT,
    facility_id   TEXT,
    occupied      BOOLEAN,
    last_updated  TIMESTAMPTZ
)
```

**API Gateway → Dashboard**

The API Gateway endpoint calls a Lambda that queries `current_room_occupancy`
and returns:

```json
{
  "facility_id":    "fac_001",
  "total_rooms":    3,
  "occupied_count": 1,
  "rooms": [
    { "room_id": "room_a", "occupied": true,  "last_updated": "..." },
    { "room_id": "room_b", "occupied": false, "last_updated": "..." }
  ]
}
```

The dashboard polls this endpoint every 30 seconds.

### 9.2 Regenerating radar_pb2.py

If you see this error when starting `main.py`:

```text
[CloudPublisher] disabled — could not import library.radar_pb2 ...
generated code is out of date
```

SSH into the Jetson and regenerate:

```bash
cd "Replicating Thesis With OOB DEMO/MMWave_Radar_Human_Tracking_and_Fall_detection"
python3 -m grpc_tools.protoc \
  --python_out=library \
  --proto_path=library \
  library/radar.proto
```

You should not need to do this on a normal session restart.

### 9.3 config_wireless_2R.py — required settings

`start_pipeline.py` verifies these before starting `main.py`. If either is
wrong the script will stop and tell you.

```python
CLOUD_PUBLISHER_CFG = {
    'enable'        : True,        # must be True
    'bridge_host'   : '127.0.0.1',
    'bridge_port'   : 9000,
    'publish_stage' : 'fused',     # or 'per_radar'
    'gzip_payload'  : True,        # must match bridge expectation
}
```

---

## 10. Fall Detection Pipeline — Deep Dive

### 10.1 Data flow detail

**main.py → FallPublisher (port 9001)**

`main.py` sends each processed point cloud frame as a newline-delimited JSON
object to `127.0.0.1:9001`. Each frame looks like:

```json
{
  "points": [
    { "x": 1.2, "y": 3.4, "z": 0.8, "v": -0.3, "snr": 14.2 },
    { "x": 1.1, "y": 3.5, "z": 0.7, "v": -0.2, "snr": 13.8 }
  ]
}
```

**FallPublisher — detection and publish**

`FallPublisher` receives frames on port 9001, calls
`run_fall_detection(points)`, and if the result exceeds `FALL_THRESHOLD`
publishes an MQTT message to IoT Core:

```json
{
  "hubId":          "Jetson-RadarEdge-001",
  "fallConfidence": 0.94,
  "trackId":        "track_007",
  "timestamp":      "2024-11-14T14:32:01Z",
  "location":       { "zone": "bedroom", "x": 1.2, "y": 3.4, "z": 0.0 }
}
```

**IoT Core → Lambda (triad-fall-handler)**

IoT Core rule:

```sql
SELECT * FROM 'triad/falls/+'
```

The Lambda writes the event to DynamoDB and triggers SNS.

**DynamoDB — TriadFallEvents**

Primary key: `hubId` (partition) + `timestamp` (sort).

```bash
# Check for fall events from a specific hub:
aws dynamodb scan \
  --table-name TriadFallEvents \
  --region us-east-1 \
  --filter-expression "hubId = :h" \
  --expression-attribute-values '{":h":{"S":"Jetson-RadarEdge-001"}}' \
  --query "Count"
```

**SNS → email/SMS**

Alert email currently registered to: `dasaridyuti@gmail.com`

Fires within seconds of a confirmed fall. If it stops firing, check
subscription status with `health_check_all.py` — the `sns` check will show
`PENDING confirmation` if the subscription was reset.

### 10.2 FallDetectionClient — wiring main.py

Add this class to `main.py` on the Jetson and call
`fall_client.send(points)` wherever `main.py` produces a processed point
cloud frame:

```python
import socket, json, threading, logging
log = logging.getLogger(__name__)

class FallDetectionClient:
    def __init__(self, host="127.0.0.1", port=9001):
        self.host  = host
        self.port  = port
        self._sock = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.host, self.port))
            log.info("[FallDetectionClient] connected to fall publisher")
        except Exception as e:
            log.warning(f"[FallDetectionClient] could not connect: {e}")
            self._sock = None

    def send(self, points: list):
        if not self._sock:
            self._connect()
            if not self._sock:
                return
        try:
            payload = json.dumps({"points": points}) + "\n"
            with self._lock:
                self._sock.sendall(payload.encode())
        except Exception as e:
            log.warning(f"[FallDetectionClient] send failed: {e}")
            self._sock = None

# Initialise once at startup
fall_client = FallDetectionClient()

# Then wherever main.py produces a point cloud frame:
# fall_client.send(list_of_point_dicts)
```

### 10.3 Environment variables injected per hub

`provision_hub.py` and `deploy_all.py` inject these into each hub's
Greengrass recipe. You do not set these manually.

| Variable | Source | Example |
|---|---|---|
| `IOT_ENDPOINT` | `hubs.yaml` global | `a1kog7dzc42oa0-ats.iot.us-east-1.amazonaws.com` |
| `IOT_CERT` | Found on Jetson filesystem | `/greengrass/v2/thingCert.pem` |
| `IOT_KEY` | Found on Jetson filesystem | `/greengrass/v2/privKey.pem` |
| `IOT_ROOT_CA` | Found on Jetson filesystem | `/greengrass/v2/rootCA.pem` |
| `HUB_ID` | `thing_name` in hubs.yaml | `Jetson-RadarEdge-001` |
| `FALL_THRESHOLD` | `fall_threshold` in hubs.yaml | `0.7` |
| `POINT_CLOUD_PORT` | `point_cloud_port` in hubs.yaml | `9001` |

### 10.4 Checking fall publisher logs on the Jetson

```bash
ssh nvidia@192.168.1.101 \
  "sudo tail -f /greengrass/v2/logs/com.example.FallPublisher.log"
```

Healthy output:

```text
[FallPublisher] connected to IoT Core topic=triad/falls/Jetson-RadarEdge-001
[FallPublisher] listening for point clouds on port 9001
[FallPublisher] ready — threshold=0.7 port=9001
[FallPublisher] heartbeat — waiting for point clouds
```

### 10.5 Manual end-to-end test

Trigger a test fall event directly from the Jetson to confirm the full chain
(IoT Core → Lambda → DynamoDB → SNS) without needing a real fall.

> **Replace** `192.168.1.101` with your hub's IP from `hubs.yaml` and
> `Jetson-RadarEdge-001` with your hub's `thing_name` before running.

```bash
ssh nvidia@192.168.1.101 python3 - << 'EOF'
import paho.mqtt.client as mqtt, ssl, json, time

client = mqtt.Client()
client.tls_set("/greengrass/v2/rootCA.pem",
               certfile="/greengrass/v2/thingCert.pem",
               keyfile="/greengrass/v2/privKey.pem",
               tls_version=ssl.PROTOCOL_TLSv1_2)
client.connect("a1kog7dzc42oa0-ats.iot.us-east-1.amazonaws.com", port=8883)
client.loop_start()
time.sleep(3)
client.publish("triad/falls/Jetson-RadarEdge-001", json.dumps({
    "hubId":          "Jetson-RadarEdge-001",
    "fallConfidence": 0.99,
    "trackId":        "MANUAL-TEST",
    "timestamp":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "location":       {"zone": "test", "x": 0.0, "y": 0.0, "z": 0.0}
}))
time.sleep(2)
client.loop_stop()
EOF
```

Verify on your laptop that the event landed in DynamoDB:

```bash
aws dynamodb scan \
  --table-name TriadFallEvents \
  --region us-east-1 \
  --filter-expression "trackId = :t" \
  --expression-attribute-values '{":t":{"S":"MANUAL-TEST"}}' \
  --query "Items[-1]"
```

---

## 11. Wiring in the Fall Detection Model

`fall_publisher.py` ships with a placeholder `run_fall_detection()` function
that always returns `None`. Replace it with your real model.

### 11.1 Steps

**1. Edit fall_publisher.py on your laptop:**

```python
# Add at top of file, after existing imports:
import sys
sys.path.insert(0, "/path/to/your/model/folder")
from your_model_file import your_detection_function

# Replace the function body:
def run_fall_detection(points: list) -> dict | None:
    confidence, track_id, location = your_detection_function(points)
    if confidence >= FALL_THRESHOLD:
        return {
            "confidence": confidence,
            "track_id":   track_id,
            "location":   location,   # dict with zone, x, y, z
        }
    return None
```

**2. Bump the version in hubs.yaml:**

```yaml
greengrass_components:
  fall_publisher: "1.1.0"     # was 1.0.0
```

**3. Deploy to all hubs:**

```bash
python3 deploy_all.py --version 1.1.0
```

**4. Verify with health_check_all.py:**

```bash
python3 health_check_all.py
# Trigger a real fall in front of the radar
# Re-run; fall_events count should increment
```

### 11.2 Model interface contract

Your detection function must return three values:

| Return value | Type | Description |
|---|---|---|
| `confidence` | `float` | 0.0 – 1.0. Published if ≥ `FALL_THRESHOLD` |
| `track_id` | `str` | Unique identifier for the tracked person |
| `location` | `dict` | `{"zone": str, "x": float, "y": float, "z": float}` |

If no fall is detected, return `None` (or ensure `confidence < FALL_THRESHOLD`).

### 11.3 FALL_THRESHOLD tuning

`fall_threshold` is set per hub in `hubs.yaml`. Higher values mean fewer
false positives but more missed falls. Start at `0.7` and adjust based on
your environment.

After changing the threshold in `hubs.yaml`, bump the component version and
redeploy — the new value is injected as an environment variable at deploy
time.

---

## 12. Stopping the Pipeline

### 12.1 Normal stop (end of session)

```bash
python3 start_pipeline.py --stop
```

This SSHes into each hub and sends `SIGTERM` to the `main.py` process. It
waits for `[CloudPublisher] stopped` to appear in the log before reporting
success.

The Greengrass components (`FallPublisher` and `RadarPublisher`) keep running
between sessions — do not stop them unless you are taking the system offline
completely.

### 12.2 Full Greengrass shutdown (rare)

Only do this if you are taking the hub offline entirely:

```bash
ssh nvidia@192.168.1.101 "sudo systemctl stop greengrass"
```

To bring it back up next session, `start_pipeline.py` handles it
automatically (Step 1).

### 12.3 Emergency kill

If `start_pipeline.py --stop` fails and you need to kill `main.py`
immediately:

```bash
ssh nvidia@192.168.1.101 "sudo kill -9 \$(pgrep -f main.py)"
```

Then clear the ports if needed:

```bash
ssh nvidia@192.168.1.101 "sudo kill -9 \$(sudo lsof -iTCP:5000 -sTCP:LISTEN -n -P | awk 'NR>1{print \$2}')"
ssh nvidia@192.168.1.101 "sudo kill -9 \$(sudo lsof -iTCP:5001 -sTCP:LISTEN -n -P | awk 'NR>1{print \$2}')"
```

`start_pipeline.py` does all of this automatically on the next session
start (Step 4).

---

## 13. Troubleshooting

### 13.1 Radar / Greengrass

| Symptom | Likely cause | Fix |
|---|---|---|
| `[CloudPublisher] connect failed: Connection refused` loops | Bridge not running | Re-run `start_pipeline.py` — Step 2 will detect and fix component state |
| `[CloudPublisher] disabled — could not import library.radar_pb2` | Proto bindings out of date | SSH to Jetson; regenerate `radar_pb2.py` (see Section 9.2) |
| `Address already in use` on 5000/5001 | Old `main.py` still running | `start_pipeline.py` Step 4 kills these automatically |
| Pipeline runs forever with no `[DIAG]` lines | Radar nodes off or wrong IP | Check ESP32 is powered; verify `RDR_CFG_LIST` in config |
| `[CloudPublisher] connected` but bridge never shows `forwarded` | `gzip_payload` mismatch | Confirm `gzip_payload: True` in config and in bridge expectation |
| `start_pipeline.py` Step 3 times out waiting for heartbeat | `RadarPublisher` component not RUNNING | Check `sudo tail -n 100 /greengrass/v2/logs/greengrass.log` on Jetson |

### 13.2 Occupancy pipeline

| Symptom | Likely cause | Fix |
|---|---|---|
| `kinesis` check fails | `main.py` not running or CloudPublisher disconnected | Check `start_pipeline.py` completed successfully |
| Kinesis has records but S3 empty | `radar-transform` Lambda crashing | `aws logs tail /aws/lambda/radar-transform --since 5m --region us-east-1` |
| Lambda logs show `DB write failed` | Wrong DB credentials or RDS unreachable | Verify `DB_HOST`, `DB_PASSWORD` env vars on Lambda; check RDS security group allows port 5432 |
| `postgresql` check shows rows are stale | Lambda not writing or pipeline stalled | Check `lambda_writes` check first; if that passes, check Lambda for errors |
| Dashboard shows wrong `fixture_id` | `fixtures.id` in PostgreSQL doesn't match `radar_id` | `SELECT DISTINCT fixture_id FROM presence_events` — align `fixtures` table to match |
| API returns `total_rooms: 0` | `fixture_assignments` missing or wrong | `SELECT * FROM current_room_occupancy` — check `fixture_assignments` table |

### 13.3 Fall detection pipeline

| Symptom | Likely cause | Fix |
|---|---|---|
| `fall_events` check always 0 even after real fall | Model not wired in yet | See Section 11; `run_fall_detection()` returns `None` until replaced |
| DynamoDB 0 items even after wiring model | IoT Core rule not triggering Lambda | Check IoT Core → Message Routing → Rules; confirm `SELECT * FROM 'triad/falls/+'` and enabled |
| DynamoDB has items but no SNS email | SNS subscription pending | `health_check_all.py` `sns` check will flag this; re-confirm email at `dasaridyuti@gmail.com` |
| Lambda not writing to DynamoDB | Lambda role missing permissions | Verify `TriadFallEventLambdaRole` has `TriadLambdaDynamoSNSPolicy` attached |
| `provision_hub.py` Step 10.3 fails | Cert paths wrong or IoT Core endpoint mismatch | Manually check paths with `sudo find /greengrass/v2 -name "*.pem"` on Jetson |
| `com.example.FallPublisher` shows BROKEN | Recipe or artifact error | Check `/greengrass/v2/logs/greengrass.log`; re-run `provision_hub.py` |

### 13.4 Script-specific errors

| Error | Fix |
|---|---|
| `Hub 'X' not found in hubs.yaml` | Check spelling matches `name` field exactly |
| `paramiko.ssh_exception.NoValidConnectionsError` | Wrong IP in hubs.yaml or Jetson not reachable |
| `psycopg2 not installed` | `pip install psycopg2-binary` |
| `botocore.exceptions.NoCredentialsError` | `aws configure` or check environment variables |
| `ConflictException` on `create_component_version` | That version string already registered — bump `--version` |
| `Component version already exists — skipping registration` | Normal warning — deployment still proceeds |

---

## 14. Quick Reference

### New hub onboarding

```bash
# 1. Add block to hubs.yaml
# 2. Provision:
python3 provision_hub.py --hub 
# 3. Verify:
python3 health_check_all.py --hub 
```

### Every session

```bash
python3 start_pipeline.py       # Steps 1–7 on all hubs in parallel
# ↑ power on radar nodes when Step 6 prints the reminder
python3 health_check_all.py     # confirm end-to-end
```

### End of session

```bash
python3 start_pipeline.py --stop
```

### Update fall_publisher.py

```bash
# 1. Edit fall_publisher.py locally
# 2. Bump fall_publisher version in hubs.yaml
# 3. Deploy:
python3 deploy_all.py --version 
```

### Check a single hub

```bash
python3 health_check_all.py --hub Jetson-RadarEdge-001
```

### Manual DynamoDB fall event check

```bash
aws dynamodb scan \
  --table-name TriadFallEvents \
  --region us-east-1 \
  --filter-expression "hubId = :h" \
  --expression-attribute-values '{":h":{"S":"Jetson-RadarEdge-001"}}' \
  --query "Count"
```

### Manual API check

```bash
curl "https://xto6udtaqd.execute-api.us-east-1.amazonaws.com/occupancy?facility_id=fac_001"
```

### Tail Jetson logs remotely

```bash
# main.py output:
ssh nvidia@ "tail -f /tmp/main_py_.log"

# Bridge (RadarPublisher):
ssh nvidia@ "sudo tail -f /greengrass/v2/logs/com.example.RadarPublisher.log"

# Fall publisher:
ssh nvidia@ "sudo tail -f /greengrass/v2/logs/com.example.FallPublisher.log"

# Greengrass core:
ssh nvidia@ "sudo tail -f /greengrass/v2/logs/greengrass.log"
```
