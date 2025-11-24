#!/usr/bin/env bash
set -e

# Usage: ./scripts/run-cache-service.sh c2

NAME="$1"
if [ -z "$NAME" ]; then
  echo "Usage: $0 <instance_name_suffix>"
  echo "Example: $0 c2"
  exit 1
fi

STACK_NAME="haystackfilesystem"
NETWORK="${STACK_NAME}_default"
IMAGE="${STACK_NAME}-cache-service"

echo ">>> Starting cache-service instance"
echo "    Container name : cache-service-${NAME}"
echo "    Network        : ${NETWORK}"
echo "    Image          : ${IMAGE}"

if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "ERROR: Docker network '${NETWORK}' not found."
  echo "Run 'docker compose up' at least once first."
  exit 1
fi

docker run -d \
  --name "cache-service-${NAME}" \
  --network "${NETWORK}" \
  -e PYTHONUNBUFFERED=1 \
  "${IMAGE}"
