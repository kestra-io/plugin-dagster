# Kestra Dagster Plugin

## What

- Provides plugin components under `io.kestra.plugin.dagster`.
- Includes classes such as `TriggerRun`.

## Why

- This plugin integrates Kestra with Dagster.
- It provides tasks that trigger Dagster jobs via the GraphQL API and optionally wait for results so Kestra flows can orchestrate runs.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
