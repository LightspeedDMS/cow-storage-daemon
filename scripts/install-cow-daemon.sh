#!/bin/bash
# install-cow-daemon.sh -- Install CoW Storage Daemon on a fresh machine
#
# Idempotent: safe to re-run. Handles Rocky Linux / RHEL / Ubuntu.
#
# Usage:
#   ./install-cow-daemon.sh --storage-path /srv/cow-storage [--port PORT] [--api-key KEY]
#
# Prerequisites: SSH access, sudo privileges, reflink-capable filesystem (XFS/btrfs)
#
# What it does:
#   1. Validates reflink support on storage path filesystem
#   2. Installs system packages (python3, pip)
#   3. Installs Python dependencies from the repo
#   4. Generates API key (or uses provided one)
#   5. Creates config.json
#   6. Creates and enables systemd service
#   7. Starts the daemon and validates health
#   8. Prints NFS export instructions

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVICE_NAME="cow-storage-daemon"
SERVICE_USER=""  # Will be set to current user
CONFIG_DIR="/etc/cow-storage-daemon"
LOG_DIR="/var/log/cow-storage-daemon"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

STORAGE_PATH=""
PORT=8081
API_KEY=""
DRY_RUN=false
INSTALL_DIR=""  # Auto-detected from script location
PYTHON="python3"

# ---------------------------------------------------------------------------
# Colors (only if terminal supports them)
# ---------------------------------------------------------------------------

if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' NC=''
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${BLUE}[STEP]${NC}  $*"; }

run_cmd() {
    if $DRY_RUN; then
        echo "  [DRY-RUN] $*"
    else
        "$@"
    fi
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --storage-path) STORAGE_PATH="$2"; shift 2 ;;
        --port)         PORT="$2"; shift 2 ;;
        --api-key)      API_KEY="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --help|-h)
            cat <<USAGE
Usage: $0 --storage-path PATH [OPTIONS]

Required:
  --storage-path PATH   Directory on reflink-capable filesystem (XFS/btrfs)

Options:
  --port PORT           Daemon port (default: 8081)
  --api-key KEY         Use specific API key (default: auto-generate)
  --dry-run             Print what would be done without executing
  --help                Show this help

Example:
  $0 --storage-path /srv/cow-storage --port 8081
USAGE
            exit 0
            ;;
        *) log_error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate required arguments
# ---------------------------------------------------------------------------

if [[ -z "$STORAGE_PATH" ]]; then
    log_error "--storage-path is required"
    echo "Usage: $0 --storage-path /path/to/storage [--port PORT] [--api-key KEY]"
    exit 1
fi

# Refuse to run as root
if [[ "$EUID" -eq 0 ]]; then
    log_error "Do not run this script as root or with 'sudo bash'."
    echo "Run as your regular user: bash $0 $*"
    echo "The script will use sudo internally where needed."
    exit 1
fi

# Auto-detect install dir (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_USER="$(whoami)"

# ---------------------------------------------------------------------------
# Display plan
# ---------------------------------------------------------------------------

echo ""
echo "=== CoW Storage Daemon Installation ==="
echo "  Storage path:  $STORAGE_PATH"
echo "  Port:          $PORT"
echo "  API key:       ${API_KEY:-(auto-generate)}"
echo "  Install dir:   $INSTALL_DIR"
echo "  Service user:  $SERVICE_USER"
echo "  Config dir:    $CONFIG_DIR"
echo "  Dry run:       $DRY_RUN"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Validate reflink support
# ---------------------------------------------------------------------------

log_step "1/7 Validating reflink support on $STORAGE_PATH"

run_cmd mkdir -p "$STORAGE_PATH"

if ! $DRY_RUN; then
    # Get filesystem type
    FS_TYPE=$(df -T "$STORAGE_PATH" | tail -1 | awk '{print $2}')
    log_info "Filesystem type: $FS_TYPE"

    # Test actual reflink support
    TEST_DIR="$STORAGE_PATH/.reflink-test-$$"
    mkdir -p "$TEST_DIR"
    echo "reflink-test" > "$TEST_DIR/source.txt"

    if cp --reflink=always "$TEST_DIR/source.txt" "$TEST_DIR/clone.txt" 2>/dev/null; then
        log_info "Reflink support: CONFIRMED"
        rm -rf "$TEST_DIR"
    else
        rm -rf "$TEST_DIR"
        log_error "Filesystem at $STORAGE_PATH does NOT support reflinks (cp --reflink=always failed)"
        log_error "The CoW Storage Daemon requires XFS (with reflink=1), btrfs, or ext4 (with reflink)"
        log_error ""
        log_error "To check: xfs_info $STORAGE_PATH | grep reflink"
        log_error "To format with reflink: mkfs.xfs -m reflink=1 /dev/sdX"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: Detect package manager and install system deps
# ---------------------------------------------------------------------------

log_step "2/7 Installing system dependencies"

if command -v dnf &>/dev/null; then
    PKG_INSTALL="sudo dnf install -y"
elif command -v yum &>/dev/null; then
    PKG_INSTALL="sudo yum install -y"
elif command -v apt-get &>/dev/null; then
    run_cmd sudo apt-get update -qq
    PKG_INSTALL="sudo apt-get install -y"
else
    log_error "No supported package manager found (dnf/yum/apt)"
    exit 1
fi

# Check if python3 and pip are available
if ! command -v python3 &>/dev/null; then
    log_info "Installing python3..."
    run_cmd $PKG_INSTALL python3
fi

if ! python3 -m pip --version &>/dev/null 2>&1; then
    log_info "Installing pip..."
    if command -v dnf &>/dev/null || command -v yum &>/dev/null; then
        run_cmd $PKG_INSTALL python3-pip
    else
        run_cmd $PKG_INSTALL python3-pip
    fi
fi

log_info "Python: $(python3 --version 2>&1)"

# ---------------------------------------------------------------------------
# Step 3: Install Python dependencies
# ---------------------------------------------------------------------------

log_step "3/7 Installing Python dependencies"

if [[ -f "$INSTALL_DIR/pyproject.toml" ]]; then
    run_cmd python3 -m pip install --break-system-packages -e "$INSTALL_DIR" 2>/dev/null \
        || run_cmd python3 -m pip install -e "$INSTALL_DIR"
    log_info "Python dependencies installed from $INSTALL_DIR"
elif [[ -f "$INSTALL_DIR/requirements.txt" ]]; then
    run_cmd python3 -m pip install --break-system-packages -r "$INSTALL_DIR/requirements.txt" 2>/dev/null \
        || run_cmd python3 -m pip install -r "$INSTALL_DIR/requirements.txt"
    log_info "Python dependencies installed from requirements.txt"
else
    log_error "No pyproject.toml or requirements.txt found in $INSTALL_DIR"
    exit 1
fi

# Verify cow-cli entry point is available
if ! $DRY_RUN; then
    log_info "Verifying entry points..."
    if command -v cow-cli &>/dev/null || python3 -m cow_cli --version &>/dev/null; then
        log_info "cow-cli: $(python3 -m cow_cli --version 2>&1 || echo 'available')"
    else
        log_warn "cow-cli entry point not on PATH (available via: python3 -m cow_cli)"
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: Generate or validate API key
# ---------------------------------------------------------------------------

log_step "4/7 Configuring API key"

if [[ -z "$API_KEY" ]]; then
    if ! $DRY_RUN; then
        API_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
        log_info "Generated API key (SAVE THIS - shown only once):"
        echo ""
        echo "  API_KEY=$API_KEY"
        echo ""
    else
        API_KEY="<auto-generated>"
        log_info "[DRY-RUN] Would generate random 32-byte hex API key"
    fi
else
    log_info "Using provided API key"
fi

# ---------------------------------------------------------------------------
# Step 5: Create config file
# ---------------------------------------------------------------------------

log_step "5/7 Creating configuration"

run_cmd sudo mkdir -p "$CONFIG_DIR"
run_cmd sudo mkdir -p "$LOG_DIR"
run_cmd sudo chown "$SERVICE_USER:$SERVICE_USER" "$LOG_DIR"

CONFIG_FILE="$CONFIG_DIR/config.json"

if ! $DRY_RUN; then
    sudo tee "$CONFIG_FILE" > /dev/null <<CONFIGEOF
{
    "base_path": "$STORAGE_PATH",
    "port": $PORT,
    "api_key": "$API_KEY",
    "health_requires_auth": false,
    "allowed_source_roots": []
}
CONFIGEOF
    sudo chmod 600 "$CONFIG_FILE"
    sudo chown "$SERVICE_USER:$SERVICE_USER" "$CONFIG_FILE"
    log_info "Config written to $CONFIG_FILE (mode 600)"
else
    log_info "[DRY-RUN] Would write config to $CONFIG_FILE"
fi

# ---------------------------------------------------------------------------
# Step 6: Create systemd service
# ---------------------------------------------------------------------------

log_step "6/7 Creating systemd service"

UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
PYTHON_PATH="$(python3 -c "import sys; print(sys.executable)")"

if ! $DRY_RUN; then
    sudo tee "$UNIT_FILE" > /dev/null <<UNITEOF
[Unit]
Description=CoW Storage Daemon - Copy-on-Write Clone Management
After=network.target
Documentation=https://github.com/LightspeedDMS/cow-storage-daemon

[Service]
Type=simple
User=$SERVICE_USER
Group=$(id -gn "$SERVICE_USER")
WorkingDirectory=$INSTALL_DIR
Environment=PYTHONPATH=$INSTALL_DIR/src
Environment=COW_DAEMON_CONFIG=$CONFIG_FILE
ExecStart=$PYTHON_PATH -m cow_storage_daemon
Restart=on-failure
RestartSec=5
StandardOutput=append:$LOG_DIR/daemon.log
StandardError=append:$LOG_DIR/daemon-error.log

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=$STORAGE_PATH $LOG_DIR $CONFIG_DIR
ProtectHome=no

[Install]
WantedBy=multi-user.target
UNITEOF

    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE_NAME"
    log_info "Systemd service created and enabled: $UNIT_FILE"
else
    log_info "[DRY-RUN] Would create systemd unit: $UNIT_FILE"
fi

# ---------------------------------------------------------------------------
# Step 7: Start and validate
# ---------------------------------------------------------------------------

log_step "7/7 Starting daemon and validating"

if ! $DRY_RUN; then
    # Stop if already running (idempotent)
    sudo systemctl stop "$SERVICE_NAME" 2>/dev/null || true
    sudo systemctl start "$SERVICE_NAME"

    # Wait for startup
    log_info "Waiting for daemon to start..."
    sleep 2

    # Check service status
    if sudo systemctl is-active --quiet "$SERVICE_NAME"; then
        log_info "Service is running"
    else
        log_error "Service failed to start. Check logs:"
        echo "  sudo journalctl -u $SERVICE_NAME --no-pager -n 30"
        echo "  cat $LOG_DIR/daemon-error.log"
        exit 1
    fi

    # Health check
    HEALTH_RESP=$(curl -s --connect-timeout 5 "http://localhost:$PORT/api/v1/health" 2>/dev/null || echo "FAILED")
    if echo "$HEALTH_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['status']=='healthy'" 2>/dev/null; then
        log_info "Health check: PASSED"
        echo "$HEALTH_RESP" | python3 -m json.tool
    else
        log_error "Health check failed. Response: $HEALTH_RESP"
        echo "Check logs: sudo journalctl -u $SERVICE_NAME --no-pager -n 30"
        exit 1
    fi
else
    log_info "[DRY-RUN] Would start and validate service"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "=========================================="
echo "  CoW Storage Daemon Installation Complete"
echo "=========================================="
echo ""
echo "  Service:     $SERVICE_NAME"
echo "  Status:      $(systemctl is-active $SERVICE_NAME 2>/dev/null || echo 'unknown')"
echo "  Port:        $PORT"
echo "  Storage:     $STORAGE_PATH"
echo "  Config:      $CONFIG_FILE"
echo "  Logs:        $LOG_DIR/"
echo "  API key:     (stored in $CONFIG_FILE)"
echo ""
echo "  Health:      curl http://localhost:$PORT/api/v1/health"
echo "  Manage:      sudo systemctl {start|stop|restart|status} $SERVICE_NAME"
echo "  CLI:         cow-cli --help (or: python3 -m cow_cli --help)"
echo ""
echo "--- NFS Export Instructions ---"
echo ""
echo "To share the storage with CIDX/Claude Server nodes via NFS:"
echo ""
echo "  1. Install NFS server:"
echo "     sudo dnf install -y nfs-utils    # Rocky/RHEL"
echo "     sudo apt install -y nfs-kernel-server  # Ubuntu"
echo ""
echo "  2. Add to /etc/exports:"
echo "     $STORAGE_PATH  *(rw,sync,no_subtree_check,no_root_squash)"
echo ""
echo "  3. Apply and start:"
echo "     sudo exportfs -ra"
echo "     sudo systemctl enable --now nfs-server"
echo ""
echo "  4. On client nodes, mount:"
echo "     sudo mount -t nfs <this-host>:$STORAGE_PATH /mnt/cow-storage"
echo ""
echo "  Clones created by the daemon will be immediately visible to NFS clients."
echo ""
