# Tiki Product Data Ingestion Pipeline

A fault-tolerant batch ingestion pipeline for approximately **200,000 Tiki product IDs**.

The project focuses on practical Data Engineering concerns such as API ingestion, concurrency, rate control, retry handling, checkpointing, resumability, data normalization, validation, and operational logging.

---

## Architecture

```text
            products_id.csv
                  │
                  ▼
            Python ingestion pipeline
                  │
                  ├── 5 worker threads
                  ├── 1 shared global rate limiter
                  └── 5 Worker endpoints
                  │
                  ▼
      Response classification
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
     Success   Permanent  Temporary
        │         │         │
        └─────────┼─────────┘
                  ▼
             SQLite State
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
      Resume              Retry
        │                   │
        └─────────┬─────────┘
                  ▼
          Final Validation
                  │
                  ▼
            JSON Export
```

---

## Key Features

- Processes up to **200,000 product IDs**
- Extracts:
  - `id`
  - `name`
  - `url_key`
  - `price`
  - `description`
  - `images_url`
- Cleans HTML descriptions
- Normalizes image data
- Uses SQLite for per-record checkpointing
- Supports safe resume after interruption
- Separates permanent and temporary failures
- Retries transient failures after the first pass
- Uses adaptive request pacing
- Exports successful records in JSON batches
- Validates final record counts
- Uses Bash scripts for execution, reset, status, and logging

---

## 5 Threads, 5 Worker Endpoints, 1 Global Limiter

The production setup uses:

```text
5 Python threads
5 Worker endpoints
1 shared global rate limiter
```

### Why 5 Python threads?

HTTP ingestion is primarily I/O-bound. A request spends much of its lifetime waiting for network and upstream response time rather than using CPU.

With only one thread, execution is mostly sequential:

```text
request A
   ↓
wait for response
   ↓
process A
   ↓
request B
```

Using a thread pool allows the application to overlap network waiting time:

```text
Thread 1 ── request A ── waiting ───────────────┐
Thread 2 ───────────── request B ── waiting ────┤
Thread 3 ─────────────────── request C ─────────┤
                                               ▼
                                      completed results
```

This improves utilization without requiring asynchronous application code.

The production configuration uses:

```env
MAX_THREADS=5
```

Five threads were chosen as a practical balance between:

- keeping enough requests in flight to hide network latency
- avoiding unnecessary thread overhead
- keeping behavior easy to observe and debug
- preventing concurrency from becoming the main source of request pressure

The thread pool is persistent across chunks instead of being recreated for every 100 IDs.

### Why 5 Worker endpoints?

The application is configured with five Worker endpoints.

Conceptually:

```text
                       ┌── Worker Endpoint 1
                       ├── Worker Endpoint 2
Python application ────┼── Worker Endpoint 3
                       ├── Worker Endpoint 4
                       └── Worker Endpoint 5
```

The endpoints provide multiple request paths for resilience.

They are used for:

- distributing requests across available endpoints
- reducing dependency on a single endpoint
- providing limited fallback when one endpoint has a transport or upstream failure
- allowing the pipeline to continue when one endpoint temporarily becomes unavailable

The five endpoints are **not treated as five independent throughput channels**.

The design intentionally avoids this model:

```text
Worker 1 -> independent request stream
Worker 2 -> independent request stream
Worker 3 -> independent request stream
Worker 4 -> independent request stream
Worker 5 -> independent request stream
```

because that would multiply aggregate request pressure and make upstream behavior less predictable.

### Shared global rate limiter

All threads and all Worker endpoints are controlled by the same aggregate request scheduler:

```text
Thread 1 ┐
Thread 2 │
Thread 3 ├──> Shared Global Rate Limiter
Thread 4 │              │
Thread 5 ┘              ▼
                 choose endpoint
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      Worker 1      Worker 2       ... Worker 5
```

Typical configuration:

```env
MAX_THREADS=5
START_INTERVAL=1.00
MIN_INTERVAL=0.95
MAX_INTERVAL=2.00
```

For example, an interval of `0.95` seconds means the pipeline schedules approximately one new request start every `0.95` seconds **across the entire application**, not once per thread and not once per endpoint.

This distinction is important:

```text
5 threads != 5x request rate
5 endpoints != 5x request rate
```

The threads improve I/O efficiency, while the endpoints improve resilience. The shared limiter controls total request pressure.

---

## Error Handling

Each response is classified as:

### Success

Valid product data was returned and parsed successfully.

### Permanent Error

An error that should not be retried, such as:

```text
404 Not Found
```

### Temporary Error

A failure that may succeed later, such as:

```text
timeout
connection error
502 / 503 / 504
empty response
unexpected response body
temporary challenge response
```

Temporary errors are stored and retried after the first pass instead of blocking the main ingestion flow.

---

## Checkpointing and Resume

SQLite is the pipeline's operational source of truth.

Instead of saving only the last processed row, the pipeline stores state per product ID.

Conceptually:

```text
all input IDs
-
already classified IDs
=
pending IDs
```

This allows interrupted runs to resume without restarting from the beginning and prevents unresolved records from being skipped.

---

## Adaptive Request Pacing

The pipeline processes IDs in chunks and evaluates metrics such as:

```text
success
permanent errors
temporary errors
challenge rate
latency
request-start gap
chunk runtime
```

If several chunks complete cleanly, the request interval can decrease gradually.

If upstream instability appears, the pipeline increases the interval and may apply a cooldown.

Typical configuration:

```env
START_INTERVAL=1.00
MIN_INTERVAL=0.95
MAX_INTERVAL=2.00

LOW_COOLDOWN=60
MEDIUM_COOLDOWN=300
HIGH_COOLDOWN=420
```

The goal is sustainable throughput rather than maximum short-term request volume.

---

## Retry Strategy

Temporary errors are deferred until the first pass completes:

```text
First Pass
   ↓
Temporary Errors
   ↓
Retry Round 1
   ↓
Retry Round 2
```

This prevents a small number of problematic requests from blocking the full batch.

---

## Output and Validation

Successful products are exported in JSON batches:

```text
products_001.json
products_002.json
...
```

Default batch size:

```env
PRODUCTS_PER_FILE=1000
```

Before completion, the pipeline verifies:

```text
success
+
permanent_error
+
temporary_error
=
total input IDs
```

A successful run ends with:

```text
Validation: OK
```

---

## Project Structure

```text
tiki_crawl/
├── tiki_production.py
├── config.py
├── products_id.csv
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
│
├── scripts/
│   ├── run.sh
│   ├── reset.sh
│   └── status.sh
│
├── state/
├── output/
├── errors/
└── logs/
```

Runtime directories are excluded from the public repository where appropriate.

---

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create the local environment file:

```bash
cp .env.example .env
nano .env
```

Fill in the required `WORKER_URL_*` values.

Make the scripts executable:

```bash
chmod +x scripts/*.sh
```

Run a test:

```bash
./scripts/run.sh 5000
```

Run or resume the full pipeline:

```bash
./scripts/run.sh full
```

Check status:

```bash
./scripts/status.sh
```

---

## Configuration

Runtime settings are loaded from `.env` through `config.py`.

Example:

```env
WORKER_URL_1=
WORKER_URL_2=
WORKER_URL_3=
WORKER_URL_4=
WORKER_URL_5=

MAX_THREADS=5
CHUNK_SIZE=100
REQUEST_TIMEOUT=15

START_INTERVAL=1.00
MIN_INTERVAL=0.95
MAX_INTERVAL=2.00

LOW_COOLDOWN=60
MEDIUM_COOLDOWN=300
HIGH_COOLDOWN=420

PRODUCTS_PER_FILE=1000
```

`.env` contains local values and should not be committed.

`.env.example` is the public template.

---

## Git Ignore

The repository should exclude local and generated files:

```gitignore
.env
venv/
__pycache__/
*.pyc

state/
output/
errors/
logs/

.DS_Store
.vscode/
.idea/
```

Before pushing:

```bash
git status
```

Make sure `.env`, the SQLite database, logs, and generated output are not tracked.

---

## Production Result

The completed full run classified all **200,000** input IDs:

```text
Input IDs:               200,000
Successful records:      124,299
Permanent errors:         75,701
Temporary unresolved:          0
Total pipeline runtime:   55:44:35
```

The pipeline was executed across multiple resumable sessions, with runtime persisted in SQLite between sessions.

Under stable conditions, a normal chunk of **100 IDs** typically completed in approximately:

```text
Chunk size:               100 IDs
Typical chunk runtime:    ~00:01:35
Typical chunk throughput: ~63 IDs/min
```

Chunk runtime could increase when the pipeline encountered fallback requests, upstream instability, adaptive slowdowns, or cooldown periods.

The successful records were exported into JSON batches with no duplicate product IDs detected during final validation.

---

## Key Engineering Decisions

- **SQLite instead of a last-row checkpoint**  
  Tracks processing state per product and supports reliable resume.

- **Deferred retries**  
  Keeps transient failures from blocking the main ingestion pass.

- **Shared aggregate rate limiting**  
  Keeps concurrency controlled across all threads and endpoints.

- **Adaptive pacing**  
  Adjusts request timing based on observed upstream behavior.

- **Bash orchestration**  
  Separates execution, logging, reset, and status operations from Python pipeline logic.

- **Externalized configuration**  
  Keeps runtime values outside application source code.

---

## What This Project Demonstrates

This project demonstrates practical Data Engineering concepts including:

- batch ingestion
- API extraction
- schema normalization
- concurrent I/O
- rate limiting
- adaptive backoff
- retry handling
- checkpointing
- resumable workloads
- SQLite state management
- validation
- observability
- Bash orchestration
- configuration management

The main engineering challenge was not retrieving one product from an API, but making a long-running ingestion job reliable enough to process a large input set without losing progress or silently dropping records.
