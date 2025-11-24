#!/usr/bin/env bash
set -e

# Usage: ./scripts/run-storage-node.sh node2

NAME="$1"
if [ -z "$NAME" ]; then
  echo "Usage: $0 <node_name_suffix>"
  echo "Example: $0 node2"
  exit 1
fi

STACK_NAME="haystackfilesystem"   # change if your compose project name is different
NETWORK="${STACK_NAME}_default"
IMAGE="${STACK_NAME}-storage-node"

# Data dir for this instance
DATA_DIR="storage-node/data-${NAME}"
mkdir -p "$DATA_DIR"

echo ">>> Starting storage-node instance"
echo "    Container name : storage-node-${NAME}"
echo "    Data directory : ${DATA_DIR}"
echo "    Network        : ${NETWORK}"
echo "    Image          : ${IMAGE}"

# Optional: sanity check network exists
if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "ERROR: Docker network '${NETWORK}' not found."
  echo "Make sure 'docker compose up' has been run at least once."
  exit 1
fi

docker run -d \
  --name "storage-node-${NAME}" \
  --network "${NETWORK}" \
  -v "$(pwd)/${DATA_DIR}:/data" \
  -e PYTHONUNBUFFERED=1 \
  -e STORAGE_PATH=/data \
  "${IMAGE}"
