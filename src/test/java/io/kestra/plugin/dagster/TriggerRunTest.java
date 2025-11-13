package io.kestra.plugin.dagster;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerRunTest {

    @Inject
    private TestRunContextFactory runContextFactory;

    @Inject
    private EmbeddedServer embeddedServer;

    @Test
    void testTriggerRunWithoutWait() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerRun task = TriggerRun.builder()
            .baseUrl(Property.ofValue("http://localhost:" + embeddedServer.getPort() + "/graphql"))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("test_repo"))
            .jobName(Property.ofValue("test_job"))
            .wait(Property.ofValue(false))
            .build();

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is("test-run-123"));
        assertThat(output.getStatus(), is("QUEUED"));
        assertThat(output.getJobName(), is("test_job"));
        assertThat(output.getStartTime(), is(nullValue()));
        assertThat(output.getEndTime(), is(nullValue()));
    }

    @Test
    void testTriggerRunWithWaitForCompletion() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerRun task = TriggerRun.builder()
            .baseUrl(Property.ofValue("http://localhost:" + embeddedServer.getPort() + "/graphql"))
            .location(Property.ofValue("test_location"))
            .repository(Property.ofValue("test_repo"))
            .jobName(Property.ofValue("test_job_wait"))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is("test-run-456"));
        assertThat(output.getStatus(), is("SUCCESS"));
        assertThat(output.getJobName(), is("test_job_wait"));
        assertThat(output.getStartTime(), is(notNullValue()));
        assertThat(output.getEndTime(), is(notNullValue()));
    }
}