package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The relay is composed only when it is enabled, and a worker that is told to run it without a
 * database or a broker stops at startup instead of failing at its first run.
 */
class OutboxRelayWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(OutboxRelayConfiguration.class);

    @Test
    void theExchangeIsCineEventsUnlessTheEnvironmentSaysOtherwise() {
        // The default the platform gets is the one of application.yml: events.md does not name the exchange,
        // so the name is pinned here until it is agreed with the consumers.
        runner.withInitializer(new ConfigDataApplicationContextInitializer())
                .run(context -> assertEquals("cine.events",
                        context.getEnvironment().getProperty("worker.outbox-relay.amqp.exchange")));
    }

    @Test
    void theRelayIsNotComposedByDefault() {
        runner.run(context -> assertTrue(context.getBeansOfType(ScheduledJob.class).isEmpty()));
    }

    @Test
    void enablingTheRelayWithoutADatabaseStopsTheStartup() {
        runner.withPropertyValues("worker.outbox-relay.enabled=true")
                .run(context -> {
                    assertTrue(context.getStartupFailure() != null);
                    assertTrue(context.getStartupFailure().getMessage().contains("RELAY_DATABASE_URL")
                            || String.valueOf(context.getStartupFailure().getCause()).contains("RELAY_DATABASE_URL"),
                            () -> "startup failure: " + context.getStartupFailure());
                });
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
    void theRelayJobIsRegisteredWithItsOwnIntervalWhenItIsEnabled() {
        runner.withPropertyValues(
                        "worker.outbox-relay.enabled=true",
                        "worker.outbox-relay.interval-seconds=7",
                        "worker.outbox-relay.database.url=" + System.getenv().getOrDefault("TEST_DATABASE_URL", ""),
                        "worker.outbox-relay.amqp.url=" + System.getenv("TEST_AMQP_URL"))
                .run(context -> {
                    Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);
                    assertEquals(1, jobs.size());
                    ScheduledJob scheduled = jobs.values().iterator().next();
                    assertEquals("outbox-relay", scheduled.job().name());
                    assertEquals(Duration.ofSeconds(7), scheduled.interval());
                });
    }
}
