#!/bin/bash

# Function to check and restart stopped containers
restart_stopped_containers() {
  # Get a list of stopped containers
  stopped_containers=$(docker ps -a --filter "status=exited" --format "{{.Names}}")

  if [ -z "$stopped_containers" ]; then
    echo "No stopped containers found."
  else
    echo "Found stopped containers. Restarting them..."
    for container in $stopped_containers; do
      echo "Restarting container: $container"
      docker start "$container"  # Restart the container
    done
  fi
}

# Main monitoring loop
while true; do
  # Call the function to restart any stopped containers
  restart_stopped_containers

  # Wait for a while before checking again (e.g., 60 seconds)
  sleep 300
done
