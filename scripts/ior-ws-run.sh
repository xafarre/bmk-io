#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  IOR weak-scaling parameters (edit for your run)
#  Reference: Annex E — File system bandwidth test
#             for non-NVMe based shared parallel file systems
# ============================================================

# -- Size parameters --------------------------------------------------
# -b  Block size: contiguous bytes written/read per task (2–8 GiB)
BLOCK_SIZE="8g"
# -t  Transfer size: single I/O operation size
TRANSFER_SIZE="32m"
# -s  Segment count: number of segments (blocks) per task
SEGMENT_COUNT="1"

# -- IOR flags (comment individual lines to disable) ------------------
IOR_FLAGS=()
IOR_FLAGS+=("-w")          # Write test
IOR_FLAGS+=("-r")          # Read test
IOR_FLAGS+=("-F")          # File-per-process (one file per task)
IOR_FLAGS+=("-C")          # Reorder tasks for read — avoid client cache hits
IOR_FLAGS+=("-e")          # fsync(2) after write close — flush to storage
IOR_FLAGS+=("-g")          # Barriers between open, write/read, and close phases
#IOR_FLAGS+=("-k")          # Keep written files (needed for separate read test)
IOR_FLAGS+=("-a POSIX")    # Use the POSIX I/O API

IOR_EXTRA_ARGS="${IOR_FLAGS[*]}"

# -- Snellius Acceptance Test - Annex E --
# Memory hogging: at most 32 GiB free for buffer cache or 512-1024 MiB per task for
# overhead (MPI, stack, IOR buffers), whichever is larger.
# Computed per job after sourcing the system file (needs NODE_MEMORY_GIB).
# Free memory left on node = max(MAX_CACHE, ppn * OVERHEAD_PER_TASK).
# At low ppn the cache limit dominates; at high ppn the process overhead does.
MAX_CACHE_MIB=$((32 * 1024))      # 32 GiB in MiB
OVERHEAD_PER_TASK_MIB=1024        # 1024 MiB per task (MPI, stack, IOR buffers)

# -- Scaling ----------------------------------------------------------
#TASKS_PER_NODE=128
#NTASKS_LIST="1 2 4 8 16 32 64 128"
TASKS_PER_NODE=8
NTASKS_LIST="8 16 32 64 128 256 512"

# ============================================================
#  Parse arguments
# ============================================================
SYSTEM_FILE=""
DRY_RUN=false
CHAIN=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/ior-ws-run.sh --system=<path> [--dry-run] [--chain]

Required:
  --system=PATH    Path to system config file (e.g. systems/snellius.sh)

Optional:
  --dry-run        Generate scripts without submitting
  --chain          Submit jobs sequentially (each depends on the previous)
EOF
}

for arg in "$@"; do
  case "$arg" in
    --system=*) SYSTEM_FILE="${arg#--system=}" ;;
    --dry-run)  DRY_RUN=true ;;
    --chain)    CHAIN=true ;;
    -h|--help)  usage; exit 0 ;;
    *)          echo "Unknown argument: $arg" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$SYSTEM_FILE" ]]; then
  echo "ERROR: --system is required." >&2
  usage
  exit 1
fi

if [[ ! -f "$SYSTEM_FILE" ]]; then
  echo "ERROR: system file not found: $SYSTEM_FILE" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "$SYSTEM_FILE"

# Append case-specific subdirectory to target dir
TARGET_DIR="${TARGET_DIR}/ior-ws"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP=$(date +%Y-%m-%d_%H%M%S)
CASE_DIR="$REPO_ROOT/results/ior-ws/$CLUSTER/$FILESYSTEM/$TIMESTAMP"

# ---- Helper: convert IOR size string (e.g. 4g, 1m) to bytes ----
size_to_bytes() {
  local val="${1//[^0-9.]}"
  local unit="${1//[0-9.]}"
  case "${unit,,}" in
    k) echo "$val * 1024"         | bc | cut -d. -f1 ;;
    m) echo "$val * 1024^2"       | bc | cut -d. -f1 ;;
    g) echo "$val * 1024^3"       | bc | cut -d. -f1 ;;
    t) echo "$val * 1024^4"       | bc | cut -d. -f1 ;;
    *) echo "$val"                | cut -d. -f1 ;;
  esac
}

# ---- Helper: format bytes to human-readable ----
bytes_to_human() {
  local bytes=$1
  if   (( bytes >= 1024**4 )); then echo "$(echo "scale=2; $bytes / 1024^4" | bc) TiB"
  elif (( bytes >= 1024**3 )); then echo "$(echo "scale=2; $bytes / 1024^3" | bc) GiB"
  elif (( bytes >= 1024**2 )); then echo "$(echo "scale=2; $bytes / 1024^2" | bc) MiB"
  elif (( bytes >= 1024    )); then echo "$(echo "scale=2; $bytes / 1024"   | bc) KiB"
  else echo "$bytes B"
  fi
}

BLOCK_BYTES=$(size_to_bytes "$BLOCK_SIZE")
PER_PROCESS_BYTES=$(( BLOCK_BYTES * SEGMENT_COUNT ))

mkdir -p "$CASE_DIR"

# ---- Write case description ----
cat > "$CASE_DIR/ior-ws.md" <<EOF
# IOR Weak Scaling

cluster = $CLUSTER
filesystem = $FILESYSTEM
partition = $PARTITION
date = $TIMESTAMP
time_limit = $TIME
runner = $RUNNER

## IOR Parameters

ior = $IOR
block_size = $BLOCK_SIZE
transfer_size = $TRANSFER_SIZE
segment_count = $SEGMENT_COUNT
extra_args = $IOR_EXTRA_ARGS

## Scaling

tasks_per_node = $TASKS_PER_NODE
ntasks_list = $NTASKS_LIST
target_dir = $TARGET_DIR
EOF

echo "Case: $CASE_DIR"
echo "  Size per process: $(bytes_to_human $PER_PROCESS_BYTES) (block_size=$BLOCK_SIZE x segments=$SEGMENT_COUNT)"

# ---- Generate one SLURM script per ntasks ----
PREV_JOBID=""
for ntasks in $NTASKS_LIST; do
  nodes=$(( (ntasks + TASKS_PER_NODE - 1) / TASKS_PER_NODE ))
  ppn=$(( ntasks < TASKS_PER_NODE ? ntasks : TASKS_PER_NODE ))
  global_bytes=$(( PER_PROCESS_BYTES * ntasks ))
  job_name="ior-ws_n${ntasks}"
  script="$CASE_DIR/${job_name}.sh"

  # -M is per-node: hog % of node RAM, leaving max(cache, task overhead) free
  MEM_HOG_ARGS=""
  if [[ -n "${NODE_MEMORY_GIB:-}" ]]; then
    NODE_MEM_MIB=$(( NODE_MEMORY_GIB * 1024 ))
    TASK_OVERHEAD_MIB=$(( ppn * OVERHEAD_PER_TASK_MIB ))
    FREE_MIB=$(( MAX_CACHE_MIB > TASK_OVERHEAD_MIB ? MAX_CACHE_MIB : TASK_OVERHEAD_MIB ))
    if (( FREE_MIB < NODE_MEM_MIB )); then
      MEM_HOG_PCT=$(( (NODE_MEM_MIB - FREE_MIB) * 100 / NODE_MEM_MIB ))
      MEM_HOG_ARGS="-M ${MEM_HOG_PCT}"
    fi
  fi

  cat > "$script" <<SLURM
#!/bin/bash
#SBATCH --job-name=${job_name}
#SBATCH --nodes=${nodes}
#SBATCH --ntasks=${ntasks}
#SBATCH --ntasks-per-node=${ppn}
#SBATCH --time=${TIME}
#SBATCH --partition=${PARTITION}
#SBATCH --output=${CASE_DIR}/${job_name}_%j.out
#SBATCH --error=${CASE_DIR}/${job_name}_%j.err
#SBATCH --constraint=${FEATURES}
#SBATCH --exclusive

# --- Modules & environment ---
$(printf '%s\n' "${ENV_SETUP[@]}")

# --- IOR weak scaling ---
mkdir -p ${TARGET_DIR}/n${ntasks}
${RUNNER} ${IOR} -b ${BLOCK_SIZE} -t ${TRANSFER_SIZE} -s ${SEGMENT_COUNT} ${IOR_EXTRA_ARGS} ${MEM_HOG_ARGS} -o ${TARGET_DIR}/n${ntasks}/iorfile
SLURM

  chmod +x "$script"
  echo "  Generated: ${job_name}.sh  (ntasks=${ntasks}, global_size=$(bytes_to_human $global_bytes))"

  if ! $DRY_RUN; then
    sbatch_args=()
    if $CHAIN && [[ -n "$PREV_JOBID" ]]; then
      sbatch_args+=(--dependency=afterany:"$PREV_JOBID")
    fi
    PREV_JOBID=$(sbatch "${sbatch_args[@]}" "$script" | awk '{print $NF}')
    echo "    Submitted job $PREV_JOBID"
  fi
done

if $DRY_RUN; then
  echo "(dry-run mode — no jobs submitted)"
else
  echo ""
  echo "After jobs complete, parse with:"
  echo "  ./scripts/ior-ws-parse.sh $CASE_DIR"
fi
