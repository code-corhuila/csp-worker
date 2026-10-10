package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The outbox relay of ADR-014 against in-memory ports that behave like the real ones: the source
 * respects the position and the limit, and a write to the state changes what the next read returns.
 */
class RelayOutboxJobTest {

    private static final Instant T0 = Instant.parse("2026-10-07T12:00:00Z");

    private final RelayFakes.Clock clock = new RelayFakes.Clock(T0);
    private final RelayFakes.Source source = new RelayFakes.Source();
    private final RelayFakes.State state = new RelayFakes.State();
    private final RelayFakes.Validator validator = new RelayFakes.Validator();
    private final RelayFakes.Publisher publisher = new RelayFakes.Publisher(clock);
    private RelayOutboxJob job;

    @BeforeEach
    void setUp() {
        job = new RelayOutboxJob(source, state, validator, publisher, new RetryPolicy(() -> 0.5), clock);
    }

    @Test
    void aNewEventIsPublishedAndRecordedAsPublished() {
        OutboxEvent event = source.add("ReservationHeld", T0);

        JobResult result = job.run();

        assertEquals(1, result.processed());
        assertEquals(0, result.failed());
        assertEquals(List.of(event.id()), publisher.deliveredIds());
        assertEquals(RelayStatus.PUBLISHED, state.statusOf(event.id()));
        assertEquals(T0, state.publishedAt(event.id()));
    }

    @Test
    void theRunCorrelationIdTravelsWithTheEvent() {
        source.add("ReservationHeld", T0);

        job.run();

        assertFalse(publisher.lastRunCorrelationId().isBlank());
    }

    @Test
    void aPublishedEventIsNotPublishedAgain() {
        source.add("ReservationHeld", T0);
        job.run();
        clock.advance(Duration.ofMinutes(1));

        JobResult second = job.run();

        assertEquals(0, second.processed());
        assertEquals(1, publisher.deliveries().size());
    }

    @Test
    void anEventCommittedLateInsideTheOverlapIsStillDiscovered() {
        source.add("ReservationHeld", T0.plusSeconds(60));
        job.run();
        // a slower transaction commits now with a created_at earlier than the one already read
        OutboxEvent late = source.add("BookingConfirmed", T0.plusSeconds(30));
        clock.advance(Duration.ofMinutes(1));

        job.run();

        assertTrue(publisher.deliveredIds().contains(late.id()));
    }

    @Test
    void aBrokerThatIsDownGrowsTheAttemptsAndTheDelayUpToFiveMinutes() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        publisher.failTransiently(true);
        Instant previous = T0;

        for (int attempt = 1; attempt <= 12; attempt++) {
            clock.set(state.nextAttemptAt(event.id()).orElse(T0));
            job.run();
            assertEquals(attempt, state.attemptCount(event.id()));
            Instant next = state.nextAttemptAt(event.id()).orElseThrow();
            assertTrue(next.isAfter(previous), "the next attempt moves forward");
            assertTrue(Duration.between(clock.instant(), next).compareTo(Duration.ofMinutes(5)) <= 0);
            previous = next;
        }
        assertEquals(RelayStatus.PENDING, state.statusOf(event.id()));
    }

    @Test
    void anEventWaitsForItsNextAttemptBeforeItIsTriedAgain() {
        source.add("ReservationHeld", T0);
        publisher.failTransiently(true);
        job.run();

        job.run();

        assertEquals(1, publisher.attempts());
    }

    @Test
    void theEventIsDeliveredWhenTheBrokerReturnsAndNothingIsLost() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        publisher.failTransiently(true);
        job.run();
        publisher.failTransiently(false);
        clock.set(state.nextAttemptAt(event.id()).orElseThrow());

        JobResult result = job.run();

        assertEquals(1, result.processed());
        assertEquals(RelayStatus.PUBLISHED, state.statusOf(event.id()));
    }

    @Test
    void aFailingEventDoesNotBlockTheOnesAfterIt() {
        OutboxEvent broken = source.add("ReservationHeld", T0);
        OutboxEvent healthy = source.add("BookingConfirmed", T0.plusSeconds(1));
        publisher.failTransientlyFor(broken.id());

        JobResult result = job.run();

        assertEquals(1, result.processed());
        assertEquals(1, result.failed());
        assertEquals(RelayStatus.PUBLISHED, state.statusOf(healthy.id()));
        assertEquals(RelayStatus.PENDING, state.statusOf(broken.id()));
    }

    @Test
    void theTwentiethFailedAttemptExhaustsTheEventAndItIsNotTriedAgain() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        publisher.failTransiently(true);
        for (int attempt = 1; attempt <= 20; attempt++) {
            clock.set(state.nextAttemptAt(event.id()).orElse(T0));
            job.run();
        }

        assertEquals(RelayStatus.FAILED_EXHAUSTED, state.statusOf(event.id()));
        int attempts = publisher.attempts();
        clock.advance(Duration.ofHours(2));
        job.run();
        assertEquals(attempts, publisher.attempts());
    }

    @Test
    void anInvalidEnvelopeIsAPermanentFailureWithoutAPublishAttempt() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        validator.reject(event.id(), "payload.reservationId is missing");

        JobResult result = job.run();

        assertEquals(0, publisher.attempts());
        assertEquals(1, result.failed());
        assertEquals(RelayStatus.FAILED_PERMANENT, state.statusOf(event.id()));
        assertEquals("payload.reservationId is missing", state.lastError(event.id()));
        clock.advance(Duration.ofHours(1));
        job.run();
        assertEquals(0, publisher.attempts());
    }

    @Test
    void anUnroutableMessageIsAPermanentFailureAndIsNotRetried() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        publisher.failWith(PublishException.permanent("no queue is bound to the routing key", null));

        job.run();

        assertEquals(RelayStatus.FAILED_PERMANENT, state.statusOf(event.id()));
        clock.advance(Duration.ofHours(1));
        job.run();
        assertEquals(1, publisher.attempts());
    }

    @Test
    void aCrashBetweenThePublishAndTheStateUpdatePublishesTheSameEventIdAgain() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        state.crashOnNextPublishedMark();
        job.run();
        assertEquals(RelayStatus.PENDING, state.statusOf(event.id()));

        job.run();

        assertEquals(List.of(event.id(), event.id()), publisher.deliveredIds());
        assertEquals(RelayStatus.PUBLISHED, state.statusOf(event.id()));
    }

    @Test
    void aRunPublishesAtMostOneHundredEventsAndTheRestWaitsForTheNextRun() {
        IntStream.range(0, 250).forEach(i -> source.add("ReservationHeld", T0.plusMillis(i)));

        assertEquals(100, job.run().processed());
        assertEquals(100, job.run().processed());
        assertEquals(50, job.run().processed());
        assertEquals(0, job.run().processed());
        assertEquals(250, publisher.deliveredIds().stream().distinct().count());
    }

    @Test
    void theCursorIsNotAdvancedUntilEveryDiscoveredPageIsTracked() {
        IntStream.range(0, 150).forEach(i -> source.add("ReservationHeld", T0.plusMillis(i)));
        state.failTrackOnCall(2);

        JobResult interrupted = job.run();

        assertEquals(1, interrupted.failed());
        assertTrue(state.cursor().isEmpty(), "a crash while tracking must not leave the cursor ahead of the events");
        state.failTrackOnCall(-1);
        assertEquals(100, job.run().processed());
        clock.advance(Duration.ofSeconds(1));
        assertEquals(50, job.run().processed());
        assertEquals(150, publisher.deliveredIds().stream().distinct().count());
    }

    @Test
    void moreThanOneBatchInsideTheOverlapDoesNotStallTheDiscovery() {
        IntStream.range(0, 150).forEach(i -> source.add("ReservationHeld", T0.plusMillis(i)));
        while (job.run().processed() > 0) {
            clock.advance(Duration.ofSeconds(1));
        }
        OutboxEvent fresh = source.add("BookingConfirmed", T0.plusSeconds(10));
        clock.advance(Duration.ofSeconds(1));

        job.run();

        assertTrue(publisher.deliveredIds().contains(fresh.id()));
    }

    @Test
    void aRunStopsWhenItsTwentySecondsAreUsed() {
        IntStream.range(0, 50).forEach(i -> source.add("ReservationHeld", T0.plusMillis(i)));
        publisher.takeEachDelivery(Duration.ofSeconds(5));

        JobResult result = job.run();

        assertEquals(4, result.processed());
    }

    @Test
    void anEventThatLeftTheOutboxIsAPermanentFailure() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        state.track(List.of(event.id()), T0);
        source.remove(event.id());

        job.run();

        assertEquals(RelayStatus.FAILED_PERMANENT, state.statusOf(event.id()));
        assertEquals(0, publisher.attempts());
    }

    @Test
    void aSourceThatCannotBeReadFailsTheRunWithoutThrowing() {
        source.failReading(new IllegalStateException("database unavailable"));

        JobResult result = job.run();

        assertEquals(0, result.processed());
        assertEquals(1, result.failed());
    }

    @Test
    void theJobHasAName() {
        assertEquals("outbox-relay", job.name());
    }

    @Test
    void anEventKeepsItsIdentityWhenItIsRetried() {
        OutboxEvent event = source.add("ReservationHeld", T0);
        publisher.failTransiently(true);
        job.run();
        publisher.failTransiently(false);
        clock.set(state.nextAttemptAt(event.id()).orElseThrow());
        job.run();

        UUID delivered = publisher.deliveredIds().get(0);
        assertEquals(event.id(), delivered);
    }
}
