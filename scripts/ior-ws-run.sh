#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  IOR weak-scaling parameters (edit for your run)
# ============================================================
BLOCK_SIZE="4g"
TRANSFER_SIZE="1m"
SEGMENT_COUNT="16"
IOR_EXTRA_ARGS="-w -r -F"

TASKS_PER_NODE=128
NTASKS_LIST="1 2 4 8 16 32 64 128"

# ============================================================
#  Parse arguments
# ============================================================
SYSTEM_FILE=""
DRY_RUN=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/ior-ws-run.sh --system=<path> [--dry-run]

Required:
  --system=PATH    Path to system config file (e.g. systems/snellius.sh)

Optional:
  --dry-run        Generate scripts without submitting
EOF
}

for arg in "$@"; do
  case "$arg" in
    --system=*) SYSTEM_FILE="${arg#--system=}" ;;
    --dry-run)  DRY_RUN=true ;;
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

# ---- Generate one SLURM script per ntasks ----
for ntasks in $NTASKS_LIST; do
  nodes=$(( (ntasks + TASKS_PER_NODE - 1) / TASKS_PER_NODE ))
  ppn=$(( ntasks < TASKS_PER_NODE ? ntasks : TASKS_PER_NODE ))
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

# --- Modules & environment ---
$(printf '%s\n' "${ENV_SETUP[@]}")

# --- IOR weak scaling ---
mkdir -p ${TARGET_DIR}/n${ntasks}
${RUNNER} ${IOR} -b ${BLOCK_SIZE} -t ${TRANSFER_SIZE} -s ${SEGMENT_COUNT} ${IOR_EXTRA_ARGS} -o ${TARGET_DIR}/n${ntasks}/iorfile
SLURM

  chmod +x "$script"
  echo "  Generated: ${job_name}.sh"

  if ! $DRY_RUN; then
    sbatch "$script"
  fi
done

if $DRY_RUN; then
  echo "(dry-run mode — no jobs submitted)"
else
  echo ""
  echo "After jobs complete, parse with:"
  echo "  ./scripts/ior-ws-parse.sh $CASE_DIR"
fi
