package io.kestra.plugin.dagster;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a Dagster job run and optionally wait for completion.",
    description = "Launch a Dagster job execution via GraphQL API, poll for status updates, and retrieve the final state."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a Dagster job run and wait for completion with Bearer token authentication",
            full = true,
            code = """
                id: dagster
                namespace: company.team

                tasks:
                  - id: run_dagster_job
                    type: io.kestra.plugin.dagster.TriggerRun
                    baseUrl: http://localhost:3000/graphql
                    jobName: example_job
                    repository: __repository__
                    location: dagster_quickstart
                    wait: true
                    pollFrequency: PT1S
                    body:
                      runConfig:
                        ops:
                          example_op:
                            config:
                              param: "value"
                      tags:
                        source: kestra
                        namespace: "{{ flow.namespace }}"
                        flow: "{{ flow.id }}"
                        task: "{{ task.id }}"
                        execution: "{{ execution.id }}"
                """
        ),
        @Example(
            title = "Trigger a Dagster job without waiting for completion",
            full = true,
            code = """
                id: dagster_async
                namespace: company.team

                tasks:
                  - id: trigger_job
                    type: io.kestra.plugin.dagster.TriggerRun
                    baseUrl: https://dagster.cloud/myorg/prod/graphql
                    location: my_location
                    repository: my_repository
                    jobName: my_job
                    wait: false
                    options:
                      headers:
                        Authorization: "Bearer {{ secret('DAGSTER_TOKEN') }}"
                """
        )
    }
)
public class TriggerRun extends Task implements RunnableTask<TriggerRun.Output> {

    private static final ObjectMapper objectMapper = JacksonMapper.ofJson();

    @Schema(
        title = "The GraphQL API endpoint URL for your Dagster deployment",
        description = "For Dagster Cloud, this is typically https://dagster.cloud/<org>/<deployment>/graphql"
    )
    @NotNull
    private Property<String> baseUrl;

    @Schema(
        title = "The repository location name"
    )
    @NotNull
    private Property<String> location;

    @Schema(
        title = "The repository name"
    )
    @NotNull
    private Property<String> repository;

    @Schema(
        title = "The job/pipeline name to trigger"
    )
    @NotNull
    private Property<String> jobName;

    @Schema(
        title = "Whether to wait for the job run to complete",
        description = "Default value is false"
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(Boolean.FALSE);

    @Schema(
        title = "The maximum total wait duration",
        description = "Maximum time to wait for the job to complete when wait is true"
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(30));

    @Schema(
        title = "Specify how often the task should poll for the job run status",
        description = "Frequency of status checks when wait is true"
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Request body containing runConfig and tags",
        description = "Body of the GraphQL request including run configuration and tags"
    )
    private Property<Map<String, Object>> body;

    @Schema(
        title = "HTTP request options including custom headers",
        description = "Configuration for HTTP client, can include headers like Authorization"
    )
    private Property<Map<String, Object>> options;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rBaseUrl = runContext.render(this.baseUrl).as(String.class).orElseThrow();
        String rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        String rRepository = runContext.render(this.repository).as(String.class).orElseThrow();
        String rJobName = runContext.render(this.jobName).as(String.class).orElseThrow();

        runContext.logger().info("Triggering Dagster job '{}' in repository '{}/{}'",
            rJobName, rLocation, rRepository);

        LaunchRunResponse launchResponse = launchRun(runContext, rBaseUrl, rLocation,
            rRepository, rJobName);

        String runId = launchResponse.getData().getLaunchPipelineExecution().getRun().getRunId();
        String status = launchResponse.getData().getLaunchPipelineExecution().getRun().getStatus();

        runContext.logger().info("Dagster run launched with ID: {}, initial status: {}", runId, status);

        Output.OutputBuilder outputBuilder = Output.builder()
            .runId(runId)
            .status(status)
            .jobName(rJobName);

        // If not waiting, return immediately
        if (!runContext.render(this.wait).as(Boolean.class).orElseThrow()) {
            return outputBuilder.build();
        }

        runContext.logger().info("Waiting for Dagster run {} to complete", runId);

        RunStatusResponse finalStatus = Await.until(
            throwSupplier(() -> {
                RunStatusResponse statusResponse = getRunStatus(runContext, rBaseUrl, runId);
                String currentStatus = statusResponse.getData().getRunOrError().getStatus();

                runContext.logger().debug("Current status for run {}: {}", runId, currentStatus);

                // Terminal states
                if ("SUCCESS".equalsIgnoreCase(currentStatus) ||
                    "FAILURE".equalsIgnoreCase(currentStatus) ||
                    "CANCELED".equalsIgnoreCase(currentStatus)) {
                    return statusResponse;
                }

                return null;
            }),
            runContext.render(this.pollFrequency).as(Duration.class).orElseThrow(),
            runContext.render(this.maxDuration).as(Duration.class).orElseThrow()
        );

        if (finalStatus == null) {
            throw new IllegalStateException("Dagster run did not complete within the specified timeout");
        }

        RunOrError runOrError = finalStatus.getData().getRunOrError();

        return outputBuilder
            .status(runOrError.getStatus())
            .startTime(convertTimestamp(runOrError.getStartTime()))
            .endTime(convertTimestamp(runOrError.getEndTime()))
                .build();
    }

    private LaunchRunResponse launchRun(RunContext runContext, String baseUrl,
                                        String location, String repository,
                                        String jobName) throws Exception {

        Map<String, Object> runConfigData = new HashMap<>();
        Map<String,Object> renderedBody = runContext.render(this.body).asMap(String.class, Object.class);
        if (renderedBody.containsKey("runConfig")) {
            runConfigData = (Map<String, Object>) runContext.render(Property.ofValue(renderedBody.get("runConfig")))
                .asMap(String.class, Object.class);
        }


        String mutation = buildLaunchMutation();
        Map<String, Object> variables = Map.of(
            "repositoryLocationName", location,
            "repositoryName", repository,
            "pipelineName", jobName,
            "runConfigData", runConfigData
        );

        String requestBody = buildGraphQLRequest(mutation, variables);

        try (HttpClient client = getHttpClient(runContext)) {
            HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
                .uri(URI.create(baseUrl))
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .body(HttpRequest.StringRequestBody.builder().content(requestBody).build());


            var renderedOptions = runContext.render(this.options).asMap(String.class, Object.class);
            if (renderedOptions.containsKey("headers") && renderedOptions.get("headers") instanceof Map) {
                Map<String, Object> headersMap = (Map<String, Object>) renderedOptions.get("headers");
                headersMap.forEach((key, value) -> requestBuilder.addHeader(key, String.valueOf(value)));
            }

            HttpRequest request = requestBuilder.build();

            HttpResponse<LaunchRunResponse> response = client.request(request, LaunchRunResponse.class);

            if (response.getStatus().getCode() != 200) {
                throw new IllegalStateException("Failed to launch Dagster run: " + response.getBody());
            }

            LaunchRunResponse launchResponse = response.getBody();

            if (launchResponse.getData().getLaunchPipelineExecution().getTypename().equals("PythonError")) {
                throw new IllegalStateException("Python error in Dagster: " +
                    launchResponse.getData().getLaunchPipelineExecution().getMessage());
            }

            return launchResponse;
        }
    }

    private RunStatusResponse getRunStatus(RunContext runContext, String baseUrl,
                                           String runId) throws Exception {

        String query = buildStatusQuery();
        Map<String, Object> variables = Map.of("runId", runId);

        String requestBody = buildGraphQLRequest(query, variables);

        try (HttpClient client = getHttpClient(runContext)) {
            HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
                .uri(URI.create(baseUrl))
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .body(HttpRequest.StringRequestBody.builder().content(requestBody).build());


            Map<String, Object> renderedOptions = runContext.render(this.options).asMap(String.class, Object.class);
            if (renderedOptions.containsKey("headers") && renderedOptions.get("headers") instanceof Map) {
                Map<String, Object> headersMap = (Map<String, Object>) renderedOptions.get("headers");
                headersMap.forEach((key, value) -> requestBuilder.addHeader(key, String.valueOf(value)));
            }


            HttpRequest request = requestBuilder.build();

            HttpResponse<RunStatusResponse> response = client.request(request, RunStatusResponse.class);

            if (response.getStatus().getCode() != 200) {
                throw new IllegalStateException("Failed to get run status: " + response.getBody());
            }

            return response.getBody();
        }
    }

    private HttpClient getHttpClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return HttpClient.builder()
            .runContext(runContext)
            .build();
    }

    private String buildLaunchMutation() {
        return """
            mutation LaunchRun($repositoryLocationName: String!, $repositoryName: String!,\s
                              $pipelineName: String!, $runConfigData: RunConfigData!) {
              launchPipelineExecution(
                executionParams: {
                  selector: {
                    repositoryLocationName: $repositoryLocationName,
                    repositoryName: $repositoryName,
                    pipelineName: $pipelineName
                  },
                  mode: "default",
                  runConfigData: $runConfigData
                }
              ) {
                __typename
                ... on LaunchRunSuccess {
                  run {
                    runId
                    status
                  }
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
           \s""";
    }

    private String buildStatusQuery() {
        return """
            query RunStatus($runId: ID!) {
              runOrError(runId: $runId) {
                __typename
                ... on Run {
                  runId
                  status
                  jobName
                  startTime
                  endTime
                }
                ... on RunNotFoundError {
                  message
                }
              }
            }
            """;
    }

    private String buildGraphQLRequest(String query, Map<String, Object> variables)
            throws JsonProcessingException {
        Map<String, Object> request = Map.of(
            "query", query,
            "variables", variables
        );
        return objectMapper.writeValueAsString(request);
    }

    private LocalDateTime convertTimestamp(Double timestamp) {
        if (timestamp == null) {
            return null;
        }
        return LocalDateTime.ofInstant(
            Instant.ofEpochMilli((long)(timestamp * 1000)),
            ZoneId.systemDefault()
        );
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The Dagster run ID"
        )
        private String runId;

        @Schema(
            title = "The current status of the run",
            description = "Possible values: QUEUED, STARTING, STARTED, SUCCESS, FAILURE, CANCELED"
        )
        private String status;

        @Schema(
            title = "The name of the job that was triggered"
        )
        private String jobName;

        @Schema(
            title = "The start time of the run"
        )
        private LocalDateTime startTime;

        @Schema(
            title = "The end time of the run"
        )
        private LocalDateTime endTime;
    }

    // GraphQL Response Models
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class LaunchRunResponse {
        private LaunchRunData data;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class LaunchRunData {
        private LaunchPipelineExecution launchPipelineExecution;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class LaunchPipelineExecution {
        @JsonProperty("__typename")
        private String typename;

        private Run run;
        private String message;
        private String stack;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Run {
        private String runId;
        private String status;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class RunStatusResponse {
        private RunStatusData data;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class RunStatusData {
        private RunOrError runOrError;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class RunOrError {
        @JsonProperty("__typename")
        private String typename;

        private String runId;
        private String status;
        private String jobName;
        private Double startTime;
        private Double endTime;
        private String message;
    }
}
