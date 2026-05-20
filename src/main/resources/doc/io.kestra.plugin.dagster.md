# How to use the Dagster plugin

Trigger Dagster job runs from Kestra flows and optionally wait for completion.

## Authentication

Set `baseUrl` to your Dagster GraphQL endpoint (e.g. `http://localhost:3000/graphql` for local, or your Dagster Cloud deployment URL). Pass your authentication token via `options.headers.Authorization` as a Bearer token. Store it in a [secret](https://kestra.io/docs/concepts/secret).

## Tasks

`TriggerRun` launches a Dagster job — set `location` (the code location name), `repository`, and `jobName`. Pass run configuration and tags via `body`. By default the task fires and returns immediately; set `wait: true` to poll until the run reaches a terminal state. Control polling with `pollFrequency` (default 5 seconds) and `maxDuration` (default 30 minutes). The output includes `runId`, `status`, `startTime`, and `endTime`.
