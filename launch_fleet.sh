#!/usr/bin/env bash
#
# launch_fleet.sh -- build and launch this machine's workers (Linux / Proxmox CT).
#
# Starts N model-major scoring clients (one per CPU by default) plus island
# workers, all pointed at the task server. Containers auto-restart on reboot.
#
# Quick start on a fresh machine:
#   1) install Docker (the CT needs nesting enabled to run Docker in an LXC)
#   2) git clone <repo> && cd transcript_analysis
#   3) create config/db_config.json   (gitignored, so copy it onto the machine)
#   4) ./launch_fleet.sh
#
# Usage:
#   ./launch_fleet.sh [num_clients] [num_island_workers]
#   SERVER_HOST=192.168.1.204 ./launch_fleet.sh        # override server ip
#   SEED=1 ./launch_fleet.sh                            # ALSO seed island tasks
#
# Run SEED=1 on exactly ONE machine, ONE time, to populate island_task_table
# from the existing scores. Every other machine just runs ./launch_fleet.sh.
#
set -euo pipefail
cd "$(dirname "$0")"

SERVER_HOST="${SERVER_HOST:-192.168.1.204}"
CLIENTS="${1:-$(nproc)}"          # default: one scoring client per logical CPU
ISLANDS="${2:-1}"                 # default: 1 island worker (islands are light)
SEED="${SEED:-0}"
HOST="$(hostname | tr -cd 'a-zA-Z0-9-')"

if [ ! -f config/db_config.json ]; then
  echo "ERROR: config/db_config.json not found. Copy it onto this machine first." >&2
  exit 1
fi

echo ">> Building images (scoring client + island worker)..."
docker build -t transcript-client        -f client/Dockerfile .
docker build -t transcript-island-worker -f island_worker/Dockerfile .

if [ "$SEED" = "1" ]; then
  echo ">> Seeding island tasks from existing scores (one-time)..."
  docker run --rm -v "$PWD/config:/app/config:ro" \
    --entrypoint python transcript-island-worker setup_island_tables.py --refresh
fi

echo ">> Launching $CLIENTS scoring client(s) + $ISLANDS island worker(s) -> server $SERVER_HOST"
for i in $(seq 1 "$CLIENTS"); do
  name="${HOST}-client-$i"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --restart unless-stopped --name "$name" \
    -e SERVER_HOST="$SERVER_HOST" \
    -e MACHINE_NAME="${HOST}-c$i" \
    -e MODEL_CACHE_SIZE=1 \
    -v "$PWD/config:/app/config:ro" \
    transcript-client >/dev/null
done

for i in $(seq 1 "$ISLANDS"); do
  name="${HOST}-island-$i"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --restart unless-stopped --name "$name" \
    -e WORKER_NAME="${HOST}-isl$i" \
    -v "$PWD/config:/app/config:ro" \
    transcript-island-worker >/dev/null
done

echo ">> Running. Containers:"
docker ps --filter "name=${HOST}-" --format "   {{.Names}}\t{{.Status}}"
echo ">> Tail a client:  docker logs -f ${HOST}-client-1"
echo ">> Stop all:       docker rm -f \$(docker ps -q --filter name=${HOST}-)"
