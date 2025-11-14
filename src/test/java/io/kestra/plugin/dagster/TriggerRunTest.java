package io.kestra.plugin.dagster;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
            .pollFrequency(Property.ofValue(Duration.ofSeconds(1)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(notNullValue()));
        assertThat(output.getStatus(), is("SUCCESS"));
        assertThat(output.getJobName(), is("test_job_wait"));
        assertThat(output.getStartTime(), is(notNullValue()));
        assertThat(output.getEndTime(), is(notNullValue()));
    }
}