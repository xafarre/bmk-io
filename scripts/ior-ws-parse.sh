#!/usr/bin/env bash
set -euo pipefail

CASE_DIR="${1:?Usage: $0 <case-directory>}"

if [[ ! -d "$CASE_DIR" ]]; then
  echo "ERROR: directory not found: $CASE_DIR" >&2
  exit 1
fi

DAT="$CASE_DIR/ior-ws.dat"

echo "# ntasks  write_MiB_s  read_MiB_s" > "$DAT"

for f in "$CASE_DIR"/ior-ws_n*_*.out; do
  [[ -f "$f" ]] || continue

  # Extract ntasks from filename: ior-ws_n<N>_<jobid>.out
  base="$(basename "$f" .out)"
  ntasks="${base#ior-ws_n}"
  ntasks="${ntasks%%_*}"

  # IOR summary table format: "write  1234.56 ..." / "read  2345.67 ..."
  write_bw=$(awk '/^write[[:space:]]/ {print $2; exit}' "$f")
  read_bw=$(awk '/^read[[:space:]]/ {print $2; exit}' "$f")

  # Fallback: "Max Write: 1234.56 MiB/sec" format
  [[ -z "$write_bw" ]] && write_bw=$(awk '/^Max Write:/ {print $3; exit}' "$f")
  [[ -z "$read_bw" ]]  && read_bw=$(awk '/^Max Read:/  {print $3; exit}' "$f")

  write_bw="${write_bw:-0}"
  read_bw="${read_bw:-0}"

  printf "%-8s %-12s %s\n" "$ntasks" "$write_bw" "$read_bw" >> "$DAT"
done

# Sort by ntasks (preserve header)
{ head -1 "$DAT"; tail -n +2 "$DAT" | sort -n; } > "$DAT.tmp" && mv "$DAT.tmp" "$DAT"

echo "Wrote: $DAT"
echo ""
echo "Plot with:"
echo "  gnuplot -e \"case_dir='$CASE_DIR'\" ./scripts/ior-ws-plot.gpi"
