#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  MDTEST weak-scaling parameters (edit for your run)
#  Reference: Annex E — Metadata handling system calls test
# ============================================================

# -- MDTEST flags (fixed as per annex) -------------------------
MDTEST_BRANCH=3               # Branching factor of hierarchical directory structure
MDTEST_DEPTH=3                # Hierarchical tree depth (z)
MDTEST_TOTAL_ITEMS=1000       # Target total items per task (-n)
MDTEST_ITERATIONS=4           # Number of iterations

# Compute MDTEST item distribution for the requested tree
if (( MDTEST_BRANCH > 1 )); then
  MDTEST_TREE_NODES=$(( (MDTEST_BRANCH ** (MDTEST_DEPTH + 1) - 1) / (MDTEST_BRANCH - 1) ))
else
  MDTEST_TREE_NODES=$(( MDTEST_DEPTH + 1 ))
fi
MDTEST_ITEMS_PER_NODE=$(( MDTEST_TOTAL_ITEMS / MDTEST_TREE_NODES ))
MDTEST_EFFECTIVE_TOTAL_ITEMS=$(( MDTEST_ITEMS_PER_NODE * MDTEST_TREE_NODES ))
MDTEST_REMAINDER_ITEMS=$(( MDTEST_TOTAL_ITEMS % MDTEST_TREE_NODES ))

MDTEST_FLAGS=()
MDTEST_FLAGS+=("-b ${MDTEST_BRANCH}")
MDTEST_FLAGS+=("-z ${MDTEST_DEPTH}")
MDTEST_FLAGS+=("-n ${MDTEST_TOTAL_ITEMS}")
MDTEST_FLAGS+=("-u")          # Unique working directory for each task
MDTEST_FLAGS+=("-k")          # Keep written files
MDTEST_FLAGS+=("-i ${MDTEST_ITERATIONS}")
MDTEST_FLAGS+=("-v")          # Verbose output

MDTEST_EXTRA_ARGS="${MDTEST_FLAGS[*]}"

# -- Scaling ----------------------------------------------------------
TASKS_PER_NODE=128
NTASKS_LIST="1 2 4"
#TASKS_PER_NODE=8
#NTASKS_LIST="8 16 32 64 128 256 512"

# ============================================================
#  Parse arguments
# ============================================================
SYSTEM_FILE=""
DRY_RUN=false
CHAIN=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/mdtest-ws-run.sh --system=<path> [--dry-run] [--chain]

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
TARGET_DIR="${TARGET_DIR}/mdtest-ws"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP=$(date +%Y-%m-%d_%H%M%S)
CASE_DIR="$REPO_ROOT/results/mdtest-ws/$CLUSTER/$FILESYSTEM/$TIMESTAMP"

mkdir -p "$CASE_DIR"

# ---- Write case description ----
cat > "$CASE_DIR/mdtest-ws.md" <<EOF
# MDTEST Weak Scaling

cluster = $CLUSTER
filesystem = $FILESYSTEM
partition = $PARTITION
date = $TIMESTAMP
time_limit = $TIME
runner = $RUNNER

## MDTEST Parameters

mdtest = $MDTEST
extra_args = $MDTEST_EXTRA_ARGS

## Scaling

tasks_per_node = $TASKS_PER_NODE
ntasks_list = $NTASKS_LIST
target_dir = $TARGET_DIR
tree_depth = $MDTEST_DEPTH
total_items_per_task = $MDTEST_TOTAL_ITEMS
items_per_node = $MDTEST_ITEMS_PER_NODE
effective_total_items_per_task = $MDTEST_EFFECTIVE_TOTAL_ITEMS
EOF

echo "Case: $CASE_DIR"
echo "MDTEST total items per task = ${MDTEST_TOTAL_ITEMS}"
echo "MDTEST tree depth = ${MDTEST_DEPTH}"
echo "MDTEST branch factor = ${MDTEST_BRANCH}"
echo "MDTEST iterations = ${MDTEST_ITERATIONS}"
if (( MDTEST_REMAINDER_ITEMS != 0 )); then
  echo "WARNING: requested total items ${MDTEST_TOTAL_ITEMS} is not divisible by tree nodes ${MDTEST_TREE_NODES}; mdtest will use ${MDTEST_ITEMS_PER_NODE} items per node for effective total ${MDTEST_EFFECTIVE_TOTAL_ITEMS}."
fi
echo ""

# ---- Generate one SLURM script per ntasks ----
PREV_JOBID=""
for ntasks in $NTASKS_LIST; do
  nodes=$(( (ntasks + TASKS_PER_NODE - 1) / TASKS_PER_NODE ))
  ppn=$(( ntasks < TASKS_PER_NODE ? ntasks : TASKS_PER_NODE ))
  job_name="mdtest-ws_n${ntasks}"
  script="$CASE_DIR/${job_name}.sh"

  # Estimate tree sizes based on MDTEST parameters
  if (( MDTEST_BRANCH > 1 )); then
    total_tree_nodes_per_task=$(( (MDTEST_BRANCH ** (MDTEST_DEPTH + 1) - 1) / (MDTEST_BRANCH - 1) ))
  else
    total_tree_nodes_per_task=$(( MDTEST_DEPTH + 1 ))
  fi
  mdtest_items_per_node=$(( MDTEST_TOTAL_ITEMS / total_tree_nodes_per_task ))
  effective_items_per_task=$(( mdtest_items_per_node * total_tree_nodes_per_task ))
  total_dirs_per_task=$(( total_tree_nodes_per_task ))
  total_files_per_task=$(( effective_items_per_task ))
  total_items=$(( ntasks * effective_items_per_task ))

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

# --- MDTEST weak scaling ---
mkdir -p ${TARGET_DIR}/n${ntasks}
${RUNNER} ${MDTEST} ${MDTEST_EXTRA_ARGS} -d ${TARGET_DIR}/n${ntasks}
SLURM

  chmod +x "$script"
  echo "  Generated: ${job_name}.sh  (ntasks=${ntasks}, requested total items/task=${MDTEST_TOTAL_ITEMS}, items/node=${mdtest_items_per_node}, effective items/task=${effective_items_per_task}, est dirs/task=${total_dirs_per_task})"

  if ! $DRY_RUN; then
    sbatch_args=()
    if $CHAIN && [[ -n "$PREV_JOBID" ]]; then
      sbatch_args+=(--dependency=afterany:"$PREV_JOBID")
    fi
    PREV_JOBID=$(sbatch "${sbatch_args[@]}" "$script" | awk '{print $NF}')
    echo "    Submitted job $PREV_JOBID"
  fi
done

  echo "After jobs complete, parse with:"
  echo "  ./scripts/mdtest-ws-parse.sh $CASE_DIR"
