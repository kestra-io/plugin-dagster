# Kestra Dagster Plugin

## What

description = 'Dagster Plugin for Kestra Exposes 1 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Dagster, allowing orchestration of Dagster-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `dagster`

Infrastructure dependencies (Docker Compose services):

- `dagster_daemon`
- `dagster_network`
- `dagster_postgres`
- `dagster_user_code`
- `dagster_webserver`

### Key Plugin Classes

- `io.kestra.plugin.dagster.TriggerRun`

### Project Structure

```
plugin-dagster/
├── src/main/java/io/kestra/plugin/dagster/
├── src/test/java/io/kestra/plugin/dagster/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
