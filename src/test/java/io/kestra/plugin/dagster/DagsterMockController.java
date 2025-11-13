package io.kestra.plugin.dagster;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Requires(property = "kestra.test")
@Controller("/graphql")
public class DagsterMockController {

    private final AtomicInteger statusCallCount = new AtomicInteger(0);

    @Post(consumes = MediaType.APPLICATION_JSON, produces = MediaType.APPLICATION_JSON)
    public Map<String, Object> graphql(@Body Map<String, Object> request) {
        String query = (String) request.get("query");

        // Handle launch mutation
        if (query.contains("launchPipelineExecution")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> variables = (Map<String, Object>) request.get("variables");
            String pipelineName = (String) variables.get("pipelineName");

            String runId = switch (pipelineName) {
                case "test_job_wait" -> "test-run-456";
                default -> "test-run-123";
            };

            return Map.of(
                "data", Map.of(
                    "launchPipelineExecution", Map.of(
                        "__typename", "LaunchRunSuccess",
                        "run", Map.of(
                            "runId", runId,
                            "status", "QUEUED"
                        )
                    )
                )
            );
        }

        // Handle status query
        if (query.contains("runOrError")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> variables = (Map<String, Object>) request.get("variables");
            String runId = (String) variables.get("runId");

            // For test_job_wait, simulate progression from STARTED to SUCCESS
            if ("test-run-456".equals(runId)) {
                int callCount = statusCallCount.incrementAndGet();
                String status = callCount >= 2 ? "SUCCESS" : "STARTED";

                Map<String, Object> runOrErrorData = new HashMap<>();
                runOrErrorData.put("__typename", "Run");
                runOrErrorData.put("runId", runId);
                runOrErrorData.put("status", status);
                runOrErrorData.put("jobName", "test_job_wait");
                runOrErrorData.put("startTime", 1699876800.0);

                // Only add endTime if status is SUCCESS
                if (status.equals("SUCCESS")) {
                    runOrErrorData.put("endTime", 1699876900.0);
                }

                return Map.of(
                    "data", Map.of(
                        "runOrError", runOrErrorData
                    )
                );
            }

            // Default response for other runs
            return Map.of(
                "data", Map.of(
                    "runOrError", Map.of(
                        "__typename", "Run",
                        "runId", runId,
                        "status", "SUCCESS",
                        "jobName", "test_job",
                        "startTime", 1699876800.0,
                        "endTime", 1699876900.0
                    )
                )
            );
        }

        throw new IllegalArgumentException("Unknown GraphQL query");
    }
}