package io.kestra.plugin.dagster;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerRunTagsTest {
    private static final String GRAPHQL = "/graphql";
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    @RegisterExtension
    static final WireMockExtension wireMock = WireMockExtension.newInstance()
        .options(wireMockConfig().dynamicPort())
        .build();

    @Inject
    private TestRunContextFactory runContextFactory;

    @Test
    void shouldSendBodyTagsAsExecutionMetadata() throws Exception {
        wireMock.stubFor(post(urlEqualTo(GRAPHQL)).willReturn(okJson(launchResponse())));

        var flow = YamlParser.parse("""
            id: dagster
            namespace: company.team

            tasks:
              - id: run_dagster_job
                type: io.kestra.plugin.dagster.TriggerRun
                baseUrl: "%s"
                jobName: test_job
                repository: __repository__
                location: test_location
                wait: false
                body:
                  runConfig:
                    ops:
                      example_op:
                        config:
                          param: value
                  tags:
                    source: kestra
                    namespace: "{{ flow.namespace }}"
                    flow: "{{ flow.id }}"
                    task: "{{ task.id }}"
                    execution: "{{ execution.id }}"
            """.formatted(wireMock.baseUrl() + GRAPHQL), Flow.class).toBuilder()
            .tenantId(MAIN_TENANT)
            .build();
        var task = (TriggerRun) flow.getTasks().getFirst();
        var execution = TestsUtils.mockExecution(flow, Map.of());
        var runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        var output = task.run(runContext);

        assertThat(output.getRunId(), is("c019f4f3-e03d-4162-bde3-a24204677570"));
        var variables = requestVariables();
        assertThat(variables.at("/runConfigData/ops/example_op/config/param").asText(), is("value"));
        var tags = tagMap(variables.at("/executionMetadata/tags"));
        assertThat(tags, aMapWithSize(5));
        assertThat(tags, hasEntry("source", "kestra"));
        assertThat(tags, hasEntry("namespace", "company.team"));
        assertThat(tags, hasEntry("flow", "dagster"));
        assertThat(tags, hasEntry("task", "run_dagster_job"));
        assertThat(tags, hasEntry("execution", execution.getId()));
    }

    @Test
    void shouldOmitExecutionMetadataWhenBodyHasNoTags() throws Exception {
        wireMock.stubFor(post(urlEqualTo(GRAPHQL)).willReturn(okJson(launchResponse())));

        var task = TriggerRun.builder()
            .id("run_dagster_job")
            .baseUrl(Property.ofValue(wireMock.baseUrl() + GRAPHQL))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("__repository__"))
            .jobName(Property.ofValue("test_job"))
            .wait(Property.ofValue(false))
            .body(Property.ofValue(Map.of("runConfig", Map.of())))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRunId(), is("c019f4f3-e03d-4162-bde3-a24204677570"));
        var variables = requestVariables();
        var metadata = variables.get("executionMetadata");
        assertThat(metadata == null || metadata.isNull(), is(true));
        assertThat(variables.get("runConfigData").size(), is(0));
    }

    private JsonNode requestVariables() throws Exception {
        var requests = wireMock.findAll(postRequestedFor(urlEqualTo(GRAPHQL)));
        assertThat(requests.size(), is(1));
        var json = MAPPER.readTree(requests.getFirst().getBodyAsString());
        assertThat(json.get("query").asText().contains("executionMetadata"), is(true));
        return json.get("variables");
    }

    private static Map<String, String> tagMap(JsonNode tags) {
        var values = new HashMap<String, String>();
        tags.forEach(tag -> values.put(tag.get("key").asText(), tag.get("value").asText()));
        return values;
    }

    private static String launchResponse() {
        return """
            {
              "data": {
                "launchPipelineExecution": {
                  "__typename": "LaunchRunSuccess",
                  "run": {
                    "runId": "c019f4f3-e03d-4162-bde3-a24204677570",
                    "status": "QUEUED"
                  }
                }
              }
            }
            """;
    }
}
