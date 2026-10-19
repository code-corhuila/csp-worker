package co.edu.corhuila.csp.worker.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.RunLock;
import co.edu.corhuila.csp.worker.application.usecase.RelayOutboxJob;
import co.edu.corhuila.csp.worker.application.usecase.RetryPolicy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** The instance lock of the relay against a real PostgreSQL, and two relays that compete for it. */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
class JdbcAdvisoryRunLockTest {

    private JdbcTemplate jdbc;
    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        jdbc = PostgresSupport.freshDatabase();
        dataSource = new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL"));
    }

    private RunLock instance() {
        return new JdbcAdvisoryRunLock(dataSource);
    }

    @Test
    void aSecondInstanceDoesNotGetTheLockWhileTheFirstHoldsIt() {
        try (RunLock.Lease first = instance().tryAcquire().orElseThrow()) {
            assertTrue(instance().tryAcquire().isEmpty());
        }
    }

    @Test
    void theLockCanBeTakenAgainOnceItIsReleased() {
        instance().tryAcquire().orElseThrow().close();

        Optional<RunLock.Lease> again = instance().tryAcquire();

        assertTrue(again.isPresent());
        again.get().close();
    }

    @Test
    void theHolderReleasesTheLockWhenItsConnectionIsKilled() throws Exception {
        RunLock.Lease holder = instance().tryAcquire().orElseThrow();
        assertTrue(instance().tryAcquire().isEmpty());

        jdbc.queryForList("SELECT pg_terminate_backend(pid) FROM pg_locks "
                + "WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()");

        Optional<RunLock.Lease> taken = Optional.empty();
        for (int attempt = 0; attempt < 50 && taken.isEmpty(); attempt++) {
            taken = instance().tryAcquire();
            if (taken.isEmpty()) {
                Thread.sleep(100);
            }
        }
        assertTrue(taken.isPresent(), "the lock of a killed connection must be released");
        taken.get().close();
        holder.close();
    }

    @Test
    void twoRelaysThatRunAtTheSameTimePublishEachEventOnce() throws Exception {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        PostgresSupport.insertEvent(jdbc, first, "ReservationHeld", "2026-10-07T12:00:00Z", "corr-a");
        PostgresSupport.insertEvent(jdbc, second, "ReservationHeld", "2026-10-07T12:00:01Z", "corr-b");
        List<UUID> delivered = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        EventPublisher slowBroker = (event, correlationId) -> {
            delivered.add(event.id());
            publishing.countDown();
            try {
                gate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        };
        RelayOutboxJob holder = relay(slowBroker);
        RelayOutboxJob competitor = relay(slowBroker);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<JobResult> running = executor.submit(holder::run);
            assertTrue(publishing.await(10, TimeUnit.SECONDS), "the first relay must reach the broker");

            JobResult skipped = competitor.run();

            assertEquals(0, skipped.processed());
            assertEquals(0, skipped.failed());
            assertEquals(1, delivered.size(), "the competitor must not publish while the holder runs");
            gate.countDown();
            assertEquals(2, running.get(10, TimeUnit.SECONDS).processed());
            competitor.run();
            assertEquals(2, delivered.size(), "every event is published once, the competitor adds none");
            assertEquals(Set.of(first, second), Set.copyOf(delivered));
        } finally {
            gate.countDown();
            executor.shutdownNow();
        }
    }

    private RelayOutboxJob relay(EventPublisher broker) {
        EnvelopeValidator valid = event -> Optional.empty();
        return new RelayOutboxJob(new JdbcOutboxSource(jdbc), new JdbcRelayStateStore(jdbc), valid, broker,
                new RetryPolicy(() -> 0.5), Clock.systemUTC(), instance());
    }
}
