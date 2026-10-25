package io.kestra.plugin.dagster;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TriggerRunTest {

    @Inject
    private TestRunContextFactory runContextFactory;

    @Test
    void testTriggerRunWithoutWait() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerRun task = TriggerRun.builder()
            .baseUrl(Property.ofValue("http://localhost:3000/graphql"))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("__repository__"))
            .jobName(Property.ofValue("test_job"))
            .wait(Property.ofValue(false))
            .build();

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(notNullValue()));
        assertThat(output.getStatus(), is(in(List.of("QUEUED", "STARTED", "SUCCESS"))));
        assertThat(output.getJobName(), is("test_job"));
    }

    @Test
    void testTriggerRunWithWaitForCompletion() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerRun task = TriggerRun.builder()
            .baseUrl(Property.ofValue("http://localhost:3000/graphql"))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("__repository__"))
            .jobName(Property.ofValue("test_job_wait"))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofSeconds(2)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(60)))
            .build();

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(notNullValue()));
        assertThat(output.getStatus(), is("SUCCESS"));
        assertThat(output.getJobName(), is("test_job_wait"));
        assertThat(output.getStartTime(), is(notNullValue()));
        assertThat(output.getEndTime(), is(notNullValue()));
    }

    @Test
    void testTriggerRunSendsBodyTags() throws Exception {
        var flow = YamlParser.parse("""
            id: dagster
            namespace: company.team

            tasks:
              - id: run_dagster_job
                type: io.kestra.plugin.dagster.TriggerRun
                baseUrl: http://localhost:3000/graphql
                jobName: test_job
                repository: __repository__
                location: test_location
                wait: false
                body:
                  tags:
                    source: kestra
                    flow: "{{ flow.id }}"
                    raw: "{{ inputs.raw }}"
                    count: 3
                    empty:
            """, Flow.class).toBuilder()
            .tenantId(MAIN_TENANT)
            .build();
        var task = (TriggerRun) flow.getTasks().getFirst();
        // an input that looks like a template must reach Dagster as is, not rendered a second time
        var execution = TestsUtils.mockExecution(flow, Map.of("raw", "{{ 7 * 7 }}"));
        var runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        var output = task.run(runContext);

        var tags = runTags(output.getRunId());
        assertThat(tags, hasEntry("source", "kestra"));
        assertThat(tags, hasEntry("flow", "dagster"));
        assertThat(tags, hasEntry("raw", "{{ 7 * 7 }}"));
        assertThat(tags, hasEntry("count", "3"));
        assertThat(tags, hasEntry("empty", ""));
    }

    @Test
    void testTriggerRunRejectsTagsThatAreNotAMap() {
        var task = TriggerRun.builder()
            .baseUrl(Property.ofValue("http://localhost:3000/graphql"))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("__repository__"))
            .jobName(Property.ofValue("test_job"))
            .wait(Property.ofValue(false))
            .body(Property.ofValue(Map.of("tags", "source=kestra")))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("`body.tags` must be a map"));
    }

    private static Map<String, String> runTags(String runId) throws Exception {
        var query = "{ runOrError(runId: \"%s\") { ... on Run { tags { key value } } } }".formatted(runId);
        var request = HttpRequest.newBuilder(URI.create("http://localhost:3000/graphql"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.ofString(JacksonMapper.ofJson().writeValueAsString(Map.of("query", query))))
            .build();
        try (var client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            var response = JacksonMapper.ofJson().readTree(client.send(request, HttpResponse.BodyHandlers.ofString()).body());
            var tags = new HashMap<String, String>();
            response.at("/data/runOrError/tags").forEach(tag -> tags.put(tag.get("key").asText(), tag.get("value").asText()));
            return tags;
        }
    }
}
