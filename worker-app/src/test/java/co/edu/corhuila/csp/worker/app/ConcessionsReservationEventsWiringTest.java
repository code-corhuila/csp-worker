package co.edu.corhuila.csp.worker.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The job that settles the concession order of a reservation is composed only when its own flag is on, and a
 * worker told to run it without a broker stops at startup. The broker is not contacted while the context is
 * built, so these tests need no broker.
 */
class ConcessionsReservationEventsWiringTest {

    private static final String BROKER = "worker.concessions-reservation-events.amqp.url=amqp://guest:guest@127.0.0.1:1";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ConcessionsReservationEventsConfiguration.class)
            .withBean(ConcessionsOrdersApi.class, () -> mock(ConcessionsOrdersApi.class));

    @Test
    void theJobIsOffByDefault() {
        runner.withInitializer(new ConfigDataApplicationContextInitializer()).run(context -> {
            assertEquals("false", context.getEnvironment().getProperty("worker.concessions-reservation-events.enabled"));
            assertTrue(context.getBeansOfType(ScheduledJob.class).isEmpty());
        });
    }

    @Test
    void enablingTheJobWithoutABrokerStopsTheStartup() {
        runner.withPropertyValues("worker.concessions-reservation-events.enabled=true").run(context -> {
            assertNotNull(context.getStartupFailure());
            assertTrue(String.valueOf(context.getStartupFailure()).contains("CONCESSIONS_AMQP_URL")
                    || String.valueOf(context.getStartupFailure().getCause()).contains("CONCESSIONS_AMQP_URL"),
                    () -> "startup failure: " + context.getStartupFailure());
        });
    }

    @Test
    void theJobIsRegisteredWithItsOwnNameAndInterval() {
        runner.withPropertyValues("worker.concessions-reservation-events.enabled=true", BROKER,
                "worker.concessions-reservation-events.interval-seconds=7").run(context -> {
                    Map<String, ScheduledJob> jobs = context.getBeansOfType(ScheduledJob.class);
                    assertEquals(1, jobs.size());
                    ScheduledJob scheduled = jobs.values().iterator().next();
                    assertEquals("concessions-reservation-events", scheduled.job().name());
                    assertEquals(Duration.ofSeconds(7), scheduled.interval());
                });
    }

    @Test
    void aBatchOrATimeThatIsNotPositiveStopsTheStartup() {
        runner.withPropertyValues("worker.concessions-reservation-events.enabled=true", BROKER,
                "worker.concessions-reservation-events.batch-size=0")
                .run(context -> assertNotNull(context.getStartupFailure()));
        runner.withPropertyValues("worker.concessions-reservation-events.enabled=true", BROKER,
                "worker.concessions-reservation-events.run-timeout-seconds=0")
                .run(context -> assertNotNull(context.getStartupFailure()));
    }
}
