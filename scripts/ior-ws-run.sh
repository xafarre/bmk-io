#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  IOR weak-scaling parameters (edit for your run)
# ============================================================
BLOCK_SIZE="4g"
TRANSFER_SIZE="1m"
SEGMENT_COUNT="4"
IOR_EXTRA_ARGS="-w -r -F"

TASKS_PER_NODE=128
NTASKS_LIST="1 2 4 8 16 32 64 128"

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
${RUNNER} ${IOR} -b ${BLOCK_SIZE} -t ${TRANSFER_SIZE} -s ${SEGMENT_COUNT} ${IOR_EXTRA_ARGS} -o ${TARGET_DIR}/n${ntasks}/iorfile
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
