#!/usr/bin/env bash
set -e

# Usage: ./scripts/run-directory-service.sh replica2

NAME="$1"
if [ -z "$NAME" ]; then
  echo "Usage: $0 <instance_name_suffix>"
  echo "Example: $0 replica2"
  exit 1
fi

STACK_NAME="haystackfilesystem"
NETWORK="${STACK_NAME}_default"
IMAGE="${STACK_NAME}-directory-service"

echo ">>> Starting directory-service instance"
echo "    Container name : directory-service-${NAME}"
echo "    Network        : ${NETWORK}"
echo "    Image          : ${IMAGE}"

if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "ERROR: Docker network '${NETWORK}' not found."
  echo "Run 'docker compose up' at least once first."
  exit 1
fi

docker run -d \
  --name "directory-service-${NAME}" \
  --network "${NETWORK}" \
  -e DATABASE_URL="postgresql://user:password@metadata_db:5432/metadata" \
  -e PYTHONUNBUFFERED=1 \
  -e HOSTNAME="directory-service-${NAME}" \
  "${IMAGE}"
