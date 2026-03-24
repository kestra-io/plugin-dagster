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
        break
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo "Waiting for Dagster webserver... ($ATTEMPT/$MAX_ATTEMPTS)"
    sleep 1
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "ERROR: Dagster webserver failed to start in time"
    docker compose -f docker-compose-ci.yml logs
    exit 1
fi

echo "Waiting for Dagster daemon and jobs to be available..."

MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    # Query for the test_job_wait job in the repository to confirm the daemon
    # and user code server are fully operational
    RESULT=$(curl -sf -X POST http://localhost:3000/graphql \
        -H "Content-Type: application/json" \
        -d '{"query": "{ repositoryOrError(repositorySelector: {repositoryLocationName: \"test_location\", repositoryName: \"__repository__\"}) { __typename ... on Repository { jobs { name } } } }"}' 2>/dev/null || echo "")

    if echo "$RESULT" | grep -q "test_job_wait"; then
        echo "Dagster jobs are available!"
        break
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo "Waiting for Dagster jobs to be available... ($ATTEMPT/$MAX_ATTEMPTS)"
    sleep 2
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "ERROR: Dagster jobs not available in time"
    docker compose -f docker-compose-ci.yml logs
    exit 1
fi

# Give the daemon a few extra seconds to fully initialize its run processing loop
echo "Waiting for Dagster daemon to stabilize..."
sleep 5

echo "Dagster is fully ready for testing!"
exit 0
