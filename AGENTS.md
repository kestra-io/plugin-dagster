# Kestra Dagster Plugin

## What

- Provides plugin components under `io.kestra.plugin.dagster`.
- Includes classes such as `TriggerRun`.

## Why

- What user problem does this solve? Teams need to trigger Dagster jobs from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Dagster steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Dagster.

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
