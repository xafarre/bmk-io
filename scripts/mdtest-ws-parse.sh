#!/usr/bin/env bash
set -euo pipefail

CASE_DIR="${1:?Usage: $0 <case-directory>}"

if [[ ! -d "$CASE_DIR" ]]; then
  echo "ERROR: directory not found: $CASE_DIR" >&2
  exit 1
fi

DAT="$CASE_DIR/mdtest-ws.dat"

echo "# ntasks  dir_create  dir_stat  dir_remove  file_create  file_stat  file_remove" > "$DAT"

for f in "$CASE_DIR"/mdtest-ws_n*_*.out; do
  [[ -f "$f" ]] || continue

  # Extract ntasks from filename: mdtest-ws_n<N>_<jobid>.out
  base="$(basename "$f" .out)"
  ntasks="${base#mdtest-ws_n}"
  ntasks="${ntasks%%_*}"

  # Extract ops/sec from verbose MDTEST output and average over all iterations if present
  dir_create=$(awk '/Directory creation:/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")
  dir_stat=$(awk '/Directory stat/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")
  dir_remove=$(awk '/Directory removal/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")
  file_create=$(awk '/File creation/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")
  file_stat=$(awk '/File stat/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")
  file_remove=$(awk '/File removal/ { if (match($0, /([0-9]+\.[0-9]+) ops\/sec/, a)) { sum += a[1]; count++ } } END { if (count) printf "%.3f", sum/count; else print 0 }' "$f")

  # Default to 0 if not found
  dir_create="${dir_create:-0}"
  dir_stat="${dir_stat:-0}"
  dir_remove="${dir_remove:-0}"
  file_create="${file_create:-0}"
  file_stat="${file_stat:-0}"
  file_remove="${file_remove:-0}"

  printf "%-8s %-12s %-12s %-12s %-12s %-12s %s\n" \
    "$ntasks" "$dir_create" "$dir_stat" "$dir_remove" "$file_create" "$file_stat" "$file_remove" >> "$DAT"
done

# Sort by ntasks (preserve header)
{ head -1 "$DAT"; tail -n +2 "$DAT" | sort -n; } > "$DAT.tmp" && mv "$DAT.tmp" "$DAT"

echo "Wrote: $DAT"
echo ""
echo "Plot with:"
echo "  gnuplot -e \"case_dir='$CASE_DIR'\" ./scripts/mdtest-ws-plot.gpi"