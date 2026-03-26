# CoW Storage Daemon

Lightweight Copy-on-Write clone management daemon providing FlexClone-equivalent
functionality over a REST API with API key authentication.

Runs on any Linux machine with a reflink-capable filesystem (XFS, btrfs). Clones
are created using `cp --reflink=always`, which is near-instant and shares disk
blocks with the source until either copy is modified.

Designed as a shared storage backend for CIDX Server and Claude Server development
clusters, replacing the ONTAP/FSx dependency for non-production environments.

## Prerequisites

- Linux with a reflink-capable filesystem (XFS with `reflink=1`, or btrfs)
- Python 3.9+
- The daemon hard-fails at startup if reflink is not supported -- no fallback

Verify reflink support:

```bash
echo test > /tmp/reflink-src
cp --reflink=always /tmp/reflink-src /tmp/reflink-dst && echo "SUPPORTED" || echo "NOT SUPPORTED"
rm -f /tmp/reflink-src /tmp/reflink-dst
```

## Quick Start (Manual)

```bash
# Clone the repo
git clone <repo-url> cow-storage-daemon
cd cow-storage-daemon

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create a config file (see Configuration section below)
cp config.json.example config.json
# Edit config.json: set base_path, api_key, allowed_source_roots

# Start the daemon
PYTHONPATH=./src COW_DAEMON_CONFIG=./config.json python3 -m cow_storage_daemon
```

The daemon listens on `0.0.0.0:8081` by default (configurable).

## Installer Script (Production)

For production-like deployments with systemd integration:

```bash
./scripts/install-cow-daemon.sh --storage-path /srv/cow-storage [--port 8081] [--api-key YOUR_KEY]
```

The installer:
1. Validates reflink support on the storage filesystem
2. Installs system packages (python3, pip)
3. Installs Python dependencies
4. Generates an API key if not provided
5. Creates `/etc/cow-storage-daemon/config.json`
6. Creates and enables a systemd service
7. Starts the daemon and validates health
8. Prints NFS export instructions for multi-node setups

Idempotent: safe to re-run. Supports Rocky Linux, RHEL, and Ubuntu.

## Configuration

Configuration is loaded from a JSON file specified by the `COW_DAEMON_CONFIG`
environment variable.

Example (`config.json.example`):

```json
{
  "base_path": "/data/cow-clones",
  "api_key": "change-me-to-a-secure-random-key",
  "db_path": "/data/cow-clones/.cow-daemon.db",
  "health_requires_auth": false,
  "allowed_source_roots": ["/data/golden-repos"],
  "port": 8081,
  "host": "0.0.0.0"
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `base_path` | Yes | -- | Root directory for clone storage. Must be on a reflink-capable filesystem. |
| `api_key` | Yes | -- | Bearer token for API authentication. Restart daemon to change. |
| `db_path` | No | `{base_path}/.cow-daemon.db` | Path to SQLite metadata database. |
| `health_requires_auth` | No | `false` | When `false`, `/api/v1/health` is accessible without authentication (for load balancer probes). |
| `allowed_source_roots` | No | `[]` (allow all) | List of directory prefixes. Clone source paths must be under one of these roots. Empty list allows any source path (a warning is logged at startup). |
| `port` | No | `8081` | TCP port to listen on. |
| `host` | No | `0.0.0.0` | Bind address. |

Settings can also be provided via environment variables with the `COW_DAEMON_` prefix
(e.g., `COW_DAEMON_BASE_PATH`), but the config file is the recommended approach.

## CLI (`cow-cli`)

A command-line interface that wraps all daemon API endpoints. Register a daemon
once, and all subsequent commands auto-inject the API token -- no more curl.

### Install

```bash
pip install -e .
# Or via the installer script (installs both daemon and CLI):
./scripts/install-cow-daemon.sh --storage-path /srv/cow-storage
```

After install, `cow-cli` is on your PATH. Alternative: `python -m cow_cli`.

### Connection Management

```bash
# Register a daemon (first connection auto-activates)
cow-cli connect prod http://cow-host:8081 --token <your-api-key>

# Register a second daemon
cow-cli connect staging http://staging:8081 --token <staging-key>

# List connections (* = active)
cow-cli connections

# Switch active daemon
cow-cli activate staging

# Update a token (rotation)
cow-cli update prod --token <new-key>

# Remove a connection
cow-cli disconnect staging
```

Connections are stored in `~/.cow-storage/config.json` (chmod 600, atomic writes).

### Clone Operations

```bash
# Create a clone (waits for completion by default)
cow-cli clone /data/golden-repos/my-repo --namespace cidx --name my-clone

# Fire-and-forget (returns job ID immediately)
cow-cli clone /data/golden-repos/my-repo --namespace cidx --name my-clone --nowait

# Check async job status
cow-cli job <job-id>

# List all clones
cow-cli list

# Filter by namespace
cow-cli list --namespace cidx

# Inspect a specific clone
cow-cli info cidx my-clone

# Delete (with confirmation prompt)
cow-cli delete cidx my-clone

# Delete without prompt
cow-cli delete cidx my-clone --force
```

### Health & Stats

```bash
# Health check (active daemon)
cow-cli health

# Health check all registered daemons
cow-cli health --all

# Storage statistics
cow-cli stats
```

### JSON Output

All commands support `--json` for scripting:

```bash
cow-cli --json list
cow-cli --json health
cow-cli --json stats
cow-cli --json info cidx my-clone
```

### All Commands

| Command | Description |
|---------|-------------|
| `connect <alias> <url> --token <token>` | Register a daemon connection |
| `connections` | List registered daemons |
| `activate <alias-or-url>` | Switch active connection |
| `update <alias> --token <token>` | Rotate API token |
| `disconnect <alias>` | Remove a connection |
| `clone <source> --namespace <ns> --name <name>` | Create a CoW clone |
| `list [--namespace <ns>]` | List clones |
| `info <namespace> <name>` | Inspect clone details |
| `delete <namespace> <name> [--force]` | Delete a clone |
| `job <job-id>` | Check async job status |
| `health [--all]` | Daemon health check |
| `stats` | Storage statistics |

No TLS certificate verification (designed for internal networks).

## REST API Reference

All endpoints are prefixed with `/api/v1`. Authentication is via `Authorization: Bearer <api_key>` header.

### GET /api/v1/health

Health check. Optionally unauthenticated (controlled by `health_requires_auth` config).

**Response** (200):
```json
{
  "status": "healthy",
  "filesystem_type": "xfs",
  "cow_method": "reflink",
  "disk_total_bytes": 319026491392,
  "disk_used_bytes": 143545552896,
  "disk_available_bytes": 175480938496,
  "uptime_seconds": 42.5
}
```

### GET /api/v1/stats

Storage statistics. Requires authentication.

**Response** (200):
```json
{
  "disk_total_bytes": 319026491392,
  "disk_used_bytes": 143545552896,
  "disk_available_bytes": 175480938496,
  "clone_count_total": 3,
  "clones_by_namespace": {
    "cidx": 2,
    "claude": 1
  }
}
```

### POST /api/v1/clones

Submit an async clone creation job. Returns immediately with a job ID. Requires authentication.

**Request body**:
```json
{
  "source_path": "/data/golden-repos/my-repo",
  "namespace": "cidx",
  "name": "cidx_clone_my-repo_1700000000"
}
```

| Field | Constraints |
|-------|-------------|
| `source_path` | Must exist on disk. Must be under an `allowed_source_roots` entry (if configured). |
| `namespace` | Alphanumeric, hyphens, underscores. Max 64 chars. |
| `name` | Alphanumeric, hyphens, underscores. Max 128 chars. |

**Response** (202 Accepted):
```json
{
  "job_id": "d73b728f-f6f5-4bf1-b79a-0889a7bc079f",
  "status": "pending"
}
```

**Errors**: 400 (validation / path not allowed), 401 (unauthorized), 409 (clone name already exists).

### GET /api/v1/jobs/{job_id}

Poll a clone job's status. Requires authentication.

**Response** (200):
```json
{
  "job_id": "d73b728f-f6f5-4bf1-b79a-0889a7bc079f",
  "status": "completed",
  "namespace": "cidx",
  "name": "cidx_clone_my-repo_1700000000",
  "source_path": "/data/golden-repos/my-repo",
  "clone_path": "cidx/cidx_clone_my-repo_1700000000",
  "error": null,
  "created_at": "2026-03-25T12:00:00Z",
  "completed_at": "2026-03-25T12:00:01Z"
}
```

Job statuses: `pending` -> `running` -> `completed` or `failed`.

The `clone_path` is relative to `base_path`. Clients prepend their NFS mount point
to get the absolute filesystem path.

**Errors**: 401 (unauthorized), 404 (job not found).

### GET /api/v1/clones

List all clones, optionally filtered by namespace. Requires authentication.

**Query parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `namespace` | No | Filter clones to this namespace only. |

**Response** (200):
```json
[
  {
    "namespace": "cidx",
    "name": "clone-test-1",
    "source_path": "/data/golden-repos/my-repo",
    "clone_path": "cidx/clone-test-1",
    "created_at": "2026-03-25T12:00:00Z",
    "size_bytes": 1048576
  }
]
```

### GET /api/v1/clones/{namespace}/{name}

Get info for a specific clone. Requires authentication.

**Response** (200): Same schema as a single item in the list response above.

**Errors**: 401 (unauthorized), 404 (clone not found).

### DELETE /api/v1/clones/{namespace}/{name}

Delete a clone (removes directory from disk and metadata from database). Requires authentication.

The daemon never auto-deletes clones. Clients are fully responsible for lifecycle management.

**Response** (200):
```json
{
  "status": "deleted",
  "namespace": "cidx",
  "name": "clone-test-1"
}
```

**Errors**: 401 (unauthorized), 404 (clone not found).

## Authentication

All endpoints except `/api/v1/health` (when `health_requires_auth` is `false`) require
a Bearer token in the `Authorization` header:

```
Authorization: Bearer your-api-key-here
```

Single API key, configured in `config.json`. Restart the daemon to change the key.
Key comparison uses `hmac.compare_digest` (constant-time) to prevent timing attacks.

**Error response** (401):
```json
{
  "error": "Missing or invalid API key",
  "code": "UNAUTHORIZED"
}
```

## Error Response Format

All errors return a consistent JSON structure:

```json
{
  "error": "Human-readable error message",
  "code": "ERROR_CODE"
}
```

Error codes: `UNAUTHORIZED`, `NOT_FOUND`, `CONFLICT`, `PATH_NOT_ALLOWED`, `VALIDATION_ERROR`, `ERROR`.

## Architecture

### Concurrency Model

Per-source `asyncio.Lock` serializes clones from the same source directory to avoid
filesystem contention. Different source directories clone in parallel. The lock map
uses LRU eviction (max 1024 entries) to bound memory.

### Clone Creation Flow

1. `POST /clones` validates input and checks for name conflicts
2. A job record is created in SQLite with status `pending`
3. An `asyncio.Task` is launched for the actual clone
4. The response returns immediately with the job ID (202)
5. The background task acquires the per-source lock, runs `cp --reflink=always -a`, records size, and updates job status to `completed` (or `failed`)
6. Client polls `GET /jobs/{job_id}` until terminal status

### Storage Layout

```
{base_path}/
  .cow-daemon.db          # SQLite metadata (jobs, clones)
  cidx/                   # Namespace directory
    clone-name-1/         # Clone (reflink copy of source)
    clone-name-2/
  claude/                 # Another namespace
    clone-name-3/
```

### NFS Integration

The daemon manages clones as subdirectories under `base_path`. When `base_path` is
NFS-exported, all cluster nodes mounting that export see clones appear automatically --
the same architectural model as ONTAP junction paths.

The daemon returns **relative** clone paths (e.g., `cidx/clone-name`). Clients prepend
their NFS mount point to construct the absolute path on their node.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/
```

### Project Structure

```
src/cow_storage_daemon/
  __main__.py              # Entry point (loads config, starts uvicorn)
  app.py                   # FastAPI application factory
  config.py                # Pydantic settings model
  api/
    auth.py                # Bearer token authentication
    models.py              # Request/response Pydantic models
    routes.py              # REST API route definitions
  core/
    clone_manager.py       # Clone lifecycle (submit, poll, delete)
    filesystem.py          # reflink copy, disk stats, fs detection
    metadata_store.py      # SQLite metadata (jobs, clones)
  health/
    health_service.py      # Health check and statistics
src/cow_cli/
  __main__.py              # python -m cow_cli support
  main.py                  # Click CLI group + global flags
  config.py                # Connection management (~/.cow-storage/config.json)
  client.py                # HTTP client with auto token injection
  output.py                # Table and JSON formatting
  commands/
    connection.py          # connect, connections, activate, update, disconnect
    clone.py               # clone, list, info, delete, job
    health.py              # health, stats
scripts/
  install-cow-daemon.sh    # Production installer with systemd
```
