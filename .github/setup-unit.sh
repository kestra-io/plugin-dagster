#!/bin/bash
set -e

echo "Starting Dagster services for testing..."

docker compose -f docker-compose-ci.yml up -d

echo "Waiting for Dagster webserver to be ready..."

MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -sf -X POST http://localhost:3000/graphql \
        -H "Content-Type: application/json" \
        -d '{"query": "{ __typename }"}' > /dev/null 2>&1; then
        echo "Dagster webserver is ready!"
        exit 0
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo "Waiting for Dagster... ($ATTEMPT/$MAX_ATTEMPTS)"
    sleep 1
done

echo "ERROR: Dagster webserver failed to start in time"
docker compose -f docker-compose-ci.yml logs
exit 1
