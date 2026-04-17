# System configuration: Snellius (SURF)
#
# Install IOR:
#   module load 2025 foss/2025a Autoconf/2.72-GCCcore-14.2.0
#   git clone https://github.com/hpc/ior.git && cd ior
#   ./bootstrap
#   ./configure --prefix=$HOME/.local
#   make && make install

CLUSTER="snellius"
PARTITION="rome"
TIME="00:60:00"
RUNNER="srun"
FEATURES="vastnfs"

# Total RAM per node (GiB)
NODE_MEMORY_GIB=256

# Modules & environment (one entry per command)
ENV_SETUP=(
  "module load 2025"
  "module load foss/2025a"
)

# IOR executable
IOR="$HOME/.local/bin/ior"
# MDTEST executable
MDTEST="$HOME/.local/bin/mdtest"

# Target filesystem
FILESYSTEM="vast"
TARGET_DIR="/vast/projects/$USER"