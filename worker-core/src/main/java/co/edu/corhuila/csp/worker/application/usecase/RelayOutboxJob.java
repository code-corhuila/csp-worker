package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.OutboxSource;
import co.edu.corhuila.csp.worker.application.port.out.PendingEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import co.edu.corhuila.csp.worker.application.port.out.RelayStateStore;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The outbox relay of ADR-014: publishes the events the booking API wrote to its outbox, with
 * read-only access to it and the publication state in the worker's own schema (Norma 5.3.11).
 *
 * <p>One run discovers new rows, then publishes the events that are due. Discovery re-reads the
 * last five minutes so a transaction that commits after a later one is not missed, and pages by
 * {@code (created_at, id)} so a window with more rows than one batch cannot stall it. Delivery is
 * at-least-once: a crash between the broker confirmation and the state update publishes the same
 * event id again, and consumers deduplicate by it (ADR-007). A run is bounded by 100 events and 20
 * seconds (Norma 5.7) and carries its own correlation id.
 */
public class RelayOutboxJob implements Job {

    static final int BATCH = 100;
    static final Duration RUN_LIMIT = Duration.ofSeconds(20);
    static final Duration OVERLAP = Duration.ofMinutes(5);
    private static final int MAX_ERROR_LENGTH = 500;

    private static final Logger log = LoggerFactory.getLogger(RelayOutboxJob.class);

    private final OutboxSource source;
    private final RelayStateStore state;
    private final EnvelopeValidator validator;
    private final EventPublisher publisher;
    private final RetryPolicy retryPolicy;
    private final Clock clock;

    public RelayOutboxJob(OutboxSource source, RelayStateStore state, EnvelopeValidator validator,
            EventPublisher publisher, RetryPolicy retryPolicy, Clock clock) {
        this.source = source;
        this.state = state;
        this.validator = validator;
        this.publisher = publisher;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
    }

    @Override
    public JobResult run() {
        String runId = UUID.randomUUID().toString();
        Instant deadline = clock.instant().plus(RUN_LIMIT);
        try {
            discover(deadline);
            return publishDue(runId, deadline);
        } catch (Exception exception) {
            log.error("outbox relay run failed: correlationId={}", runId, exception);
            return new JobResult(0, 1);
        }
    }

    @Override
    public String name() {
        return "outbox-relay";
    }

    private void discover(Instant deadline) {
        Optional<OutboxPosition> saved = state.cursor();
        OutboxPosition position = saved.map(cursor -> cursor.minus(OVERLAP)).orElse(OutboxPosition.START);
        OutboxPosition furthest = null;
        while (clock.instant().isBefore(deadline)) {
            List<OutboxEvent> page = source.discover(position, BATCH);
            if (page.isEmpty()) {
                break;
            }
            state.track(page.stream().map(OutboxEvent::id).toList(), clock.instant());
            furthest = OutboxPosition.of(page.get(page.size() - 1));
            position = furthest;
            if (page.size() < BATCH) {
                break;
            }
        }
        if (furthest != null && saved.map(furthest::isAfter).orElse(true)) {
            state.saveCursor(furthest);
        }
    }

    private JobResult publishDue(String runId, Instant deadline) {
        List<PendingEvent> due = state.duePending(clock.instant(), BATCH);
        Map<UUID, OutboxEvent> events = source.findByIds(due.stream().map(PendingEvent::eventId).toList()).stream()
                .collect(Collectors.toMap(OutboxEvent::id, Function.identity()));
        int published = 0;
        int failed = 0;
        for (PendingEvent pending : due) {
            if (!clock.instant().isBefore(deadline)) {
                break;
            }
            try {
                if (relay(pending, events.get(pending.eventId()), runId)) {
                    published++;
                } else {
                    failed++;
                }
            } catch (Exception exception) {
                log.error("outbox relay could not handle an event: eventId={}, correlationId={}",
                        pending.eventId(), runId, exception);
                failed++;
            }
        }
        log.info("outbox relay run completed: published={}, failed={}, correlationId={}", published, failed, runId);
        return new JobResult(published, failed);
    }

    /** @return true when the event was published, false when it failed for now or for good */
    private boolean relay(PendingEvent pending, OutboxEvent event, String runId) {
        if (event == null) {
            fail(pending, null, RelayStatus.FAILED_PERMANENT, "the event is no longer in the outbox", runId,
                    pending.attemptCount());
            return false;
        }
        Optional<String> invalid = validator.validate(event);
        if (invalid.isPresent()) {
            fail(pending, event, RelayStatus.FAILED_PERMANENT, invalid.get(), runId, pending.attemptCount());
            return false;
        }
        String correlationId = correlation(event, runId);
        try {
            publisher.publish(event, correlationId);
        } catch (PublishException exception) {
            if (exception.isPermanent()) {
                fail(pending, event, RelayStatus.FAILED_PERMANENT, describe(exception), runId,
                        pending.attemptCount() + 1);
            } else {
                retryLater(pending, event, exception, runId);
            }
            return false;
        }
        state.markPublished(pending.eventId(), clock.instant());
        log.info("event published: eventId={}, aggregateId={}, eventType={}, attemptCount={}, correlationId={}",
                event.id(), event.aggregateId(), event.eventType(), pending.attemptCount() + 1, correlationId);
        return true;
    }

    private void retryLater(PendingEvent pending, OutboxEvent event, PublishException exception, String runId) {
        int attempts = pending.attemptCount() + 1;
        if (retryPolicy.isExhausted(attempts)) {
            fail(pending, event, RelayStatus.FAILED_EXHAUSTED, describe(exception), runId, attempts);
            return;
        }
        Instant next = clock.instant().plus(retryPolicy.delay(attempts));
        state.markRetry(pending.eventId(), attempts, next, describe(exception));
        log.warn("event publication will be retried: eventId={}, aggregateId={}, eventType={}, attemptCount={}, "
                        + "nextAttemptAt={}, correlationId={}",
                event.id(), event.aggregateId(), event.eventType(), attempts, next, correlation(event, runId));
    }

    private void fail(PendingEvent pending, OutboxEvent event, RelayStatus status, String reason, String runId,
            int attempts) {
        state.markFailed(pending.eventId(), status, attempts, truncate(reason));
        log.error("event publication failed for good: status={}, eventId={}, aggregateId={}, eventType={}, "
                        + "attemptCount={}, reason={}, correlationId={}",
                status, pending.eventId(), event == null ? null : event.aggregateId(),
                event == null ? null : event.eventType(), attempts, reason, correlation(event, runId));
    }

    private static String correlation(OutboxEvent event, String runId) {
        return event != null && event.correlationId() != null ? event.correlationId() : runId;
    }

    /** The class and message of the failure: never the envelope, which carries data of the user. */
    private static String describe(PublishException exception) {
        return truncate(exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }

    private static String truncate(String text) {
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
