# xbmk-io

Minimal IOR weak-scaling benchmark toolkit.

## Dependencies

- bash
- gnuplot
- SLURM cluster with IOR installed

## Files

- `scripts/ior-ws-run.sh` — generate SLURM jobs and submit
- `scripts/ior-ws-parse.sh` — parse IOR outputs into a `.dat` file
- `scripts/ior-ws-plot.gpi` — gnuplot script to plot scaling results
- `systems/snellius.sh` — system config for Snellius (SURF)

## Workflow

### 1. Configure and run

Edit the IOR parameters at the top of `scripts/ior-ws-run.sh` if needed, then run with a system file:

```bash
./scripts/ior-ws-run.sh --system=systems/snellius.sh
```

This generates one SLURM script per task count, submits them, and creates a case directory:

```
results/ior-ws/<cluster>/<filesystem>/<timestamp>/
├── ior-ws.md        # parseable run metadata
├── ior-ws_n1.sh          # generated job scripts
├── ior-ws_n2.sh
├── ...
├── ior-ws_n1_<jobid>.out # IOR stdout (after completion)
└── ior-ws_n1_<jobid>.err
```

Use `--dry-run` to generate scripts without submitting.

### 2. Parse results

```bash
./scripts/ior-ws-parse.sh results/ior-ws/snellius/scratch-shared/<timestamp>
```

Produces `ior-ws.dat` in the case directory.

### 3. Plot

```bash
gnuplot -e "case_dir='results/ior-ws/snellius/scratch-shared/<timestamp>'" scripts/ior-ws-plot.gpi
```

Produces `ior-ws.png` in the case directory, with cluster/filesystem/IOR parameters in the title (read from `ior-ws-case.md`).
