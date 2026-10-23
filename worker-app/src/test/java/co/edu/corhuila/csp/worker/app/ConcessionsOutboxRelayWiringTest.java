package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The Concessions relay is composed only when its own flag is on, and a worker told to run it without a
 * database, a broker or a valid list of event types stops at startup. Neither engine is contacted while the
 * context is built, so these tests need no database or broker.
 */
class ConcessionsOutboxRelayWiringTest {

    private static final String[] ENGINES = {
            "worker.concessions-outbox-relay.enabled=true",
            "worker.outbox-relay.database.url=jdbc:postgresql://127.0.0.1:1/csp",
            "worker.outbox-relay.amqp.url=amqp://guest:guest@127.0.0.1:1"};

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ConcessionsOutboxRelayConfiguration.class);

    @Test
    void theRelayIsOffByDefaultAndPublishesOnlyTheEventWithAConsumer() {
        runner.withInitializer(new ConfigDataApplicationContextInitializer()).run(context -> {
            assertEquals("false", context.getEnvironment().getProperty("worker.concessions-outbox-relay.enabled"));
            assertEquals("ConcessionOrderConfirmed",
                    context.getEnvironment().getProperty("worker.concessions-outbox-relay.event-types"));
            assertTrue(context.getBeansOfType(ScheduledJob.class).isEmpty());
        });
    }

    @Test
    void enablingTheRelayWithoutADatabaseStopsTheStartup() {
        runner.withPropertyValues("worker.concessions-outbox-relay.enabled=true").run(context -> {
            assertNotNull(context.getStartupFailure());
            assertTrue(String.valueOf(context.getStartupFailure()).contains("RELAY_DATABASE_URL")
                    || String.valueOf(context.getStartupFailure().getCause()).contains("RELAY_DATABASE_URL"),
                    () -> "startup failure: " + context.getStartupFailure());
        });
    }

    @Test
    void theRelayIsRegisteredWithItsOwnNameAndInterval() {
        runner.withPropertyValues(ENGINES).withPropertyValues("worker.concessions-outbox-relay.interval-seconds=9")
                .run(context -> {
                    Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);
                    assertEquals(1, jobs.size());
                    ScheduledJob scheduled = jobs.values().iterator().next();
                    assertEquals("concessions-outbox-relay", scheduled.job().name());
                    assertEquals(Duration.ofSeconds(9), scheduled.interval());
                });
    }

    @Test
    void moreEventTypesCanBeEnabledBySettingAloneAndAnyOtherNameStopsTheStartup() {
        runner.withPropertyValues(ENGINES).withPropertyValues(
                        "worker.concessions-outbox-relay.event-types=ConcessionOrderConfirmed,ConcessionOrderCancelled")
                .run(context -> assertNotNull(context.getBean(ScheduledJob.class)));
        runner.withPropertyValues(ENGINES)
                .withPropertyValues("worker.concessions-outbox-relay.event-types=OrderConfirmed")
                .run(context -> assertNotNull(context.getStartupFailure()));
        runner.withPropertyValues(ENGINES).withPropertyValues("worker.concessions-outbox-relay.event-types= ")
                .run(context -> assertNotNull(context.getStartupFailure()));
    }
}
