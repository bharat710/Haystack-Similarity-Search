#!/usr/bin/env bash
set -e

# Usage: ./scripts/run-ai-worker.sh w2

NAME="$1"
if [ -z "$NAME" ]; then
  echo "Usage: $0 <instance_name_suffix>"
  echo "Example: $0 w2"
  exit 1
fi

STACK_NAME="haystackfilesystem"
NETWORK="${STACK_NAME}_default"
IMAGE="${STACK_NAME}-ai-worker"

echo ">>> Starting ai-worker instance"
echo "    Container name : ai-worker-${NAME}"
echo "    Network        : ${NETWORK}"
echo "    Image          : ${IMAGE}"

if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "ERROR: Docker network '${NETWORK}' not found."
  echo "Run 'docker compose up' at least once first."
  exit 1
fi

docker run -d \
  --name "ai-worker-${NAME}" \
  --network "${NETWORK}" \
  -e PYTHONUNBUFFERED=1 \
  -e RABBITMQ_HOST="rabbitmq" \
  -e STORAGE_NODE_URL="http://storage-node:8080" \
  -e SEARCH_SERVICE_URL="http://search-service:8000" \
  "${IMAGE}"
