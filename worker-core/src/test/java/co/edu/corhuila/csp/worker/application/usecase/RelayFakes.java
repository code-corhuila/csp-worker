package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.OutboxEvent;
import co.edu.corhuila.csp.worker.application.port.out.OutboxPosition;
import co.edu.corhuila.csp.worker.application.port.out.OutboxSource;
import co.edu.corhuila.csp.worker.application.port.out.PendingEvent;
import co.edu.corhuila.csp.worker.application.port.out.PublishException;
import co.edu.corhuila.csp.worker.application.port.out.RelayStateStore;
import co.edu.corhuila.csp.worker.application.port.out.RelayStatus;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** In-memory ports of the relay that follow the behavior of the PostgreSQL and AMQP adapters. */
final class RelayFakes {

    private RelayFakes() {
    }

    /** A clock the test moves by hand. */
    static final class Clock extends java.time.Clock {
        private Instant now;

        Clock(Instant start) {
            this.now = start;
        }

        void advance(Duration amount) {
            now = now.plus(amount);
        }

        void set(Instant instant) {
            now = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public java.time.Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /** The booking outbox: ordered by (created_at, id) like the index, and it honors the limit. */
    static final class Source implements OutboxSource {
        private final List<OutboxEvent> rows = new ArrayList<>();
        private RuntimeException readFailure;

        OutboxEvent add(String eventType, Instant createdAt) {
            UUID id = UUID.randomUUID();
            OutboxEvent event = new OutboxEvent(id, eventType, UUID.randomUUID(),
                    "{\"eventId\":\"" + id + "\",\"eventType\":\"" + eventType + "\"}", "corr-" + id, createdAt);
            rows.add(event);
            return event;
        }

        void remove(UUID id) {
            rows.removeIf(row -> row.id().equals(id));
        }

        void failReading(RuntimeException failure) {
            this.readFailure = failure;
        }

        @Override
        public List<OutboxEvent> discover(OutboxPosition after, int limit) {
            if (readFailure != null) {
                throw readFailure;
            }
            return rows.stream()
                    .sorted(Comparator.comparing(OutboxEvent::createdAt).thenComparing(row -> row.id().toString()))
                    .filter(row -> OutboxPosition.of(row).isAfter(after))
                    .limit(limit)
                    .toList();
        }

        @Override
        public List<OutboxEvent> findByIds(Collection<UUID> ids) {
            return rows.stream().filter(row -> ids.contains(row.id())).toList();
        }
    }

    /** The publication state of schema worker. */
    static final class State implements RelayStateStore {
        private record Row(RelayStatus status, int attempts, Instant nextAttemptAt, Instant publishedAt,
                String lastError) {
        }

        private final Map<UUID, Row> rows = new LinkedHashMap<>();
        private OutboxPosition cursor;
        private boolean crashOnNextPublishedMark;
        private int trackCalls;
        private int failTrackOnCall = -1;

        void crashOnNextPublishedMark() {
            this.crashOnNextPublishedMark = true;
        }

        /** The database goes away on the n-th call to track, counted from now on. */
        void failTrackOnCall(int n) {
            this.trackCalls = 0;
            this.failTrackOnCall = n;
        }

        RelayStatus statusOf(UUID id) {
            return rows.get(id).status();
        }

        int attemptCount(UUID id) {
            return rows.get(id).attempts();
        }

        Optional<Instant> nextAttemptAt(UUID id) {
            return Optional.ofNullable(rows.get(id)).map(Row::nextAttemptAt);
        }

        Instant publishedAt(UUID id) {
            return rows.get(id).publishedAt();
        }

        String lastError(UUID id) {
            return rows.get(id).lastError();
        }

        @Override
        public Optional<OutboxPosition> cursor() {
            return Optional.ofNullable(cursor);
        }

        @Override
        public void saveCursor(OutboxPosition position) {
            this.cursor = position;
        }

        @Override
        public void track(Collection<UUID> eventIds, Instant now) {
            if (++trackCalls == failTrackOnCall) {
                throw new IllegalStateException("database unavailable");
            }
            for (UUID id : eventIds) {
                rows.putIfAbsent(id, new Row(RelayStatus.PENDING, 0, now, null, null));
            }
        }

        @Override
        public List<PendingEvent> duePending(Instant now, int limit) {
            return rows.entrySet().stream()
                    .filter(entry -> entry.getValue().status() == RelayStatus.PENDING)
                    .filter(entry -> !entry.getValue().nextAttemptAt().isAfter(now))
                    .map(entry -> new PendingEvent(entry.getKey(), entry.getValue().attempts()))
                    .limit(limit)
                    .toList();
        }

        @Override
        public void markPublished(UUID eventId, Instant now) {
            if (crashOnNextPublishedMark) {
                crashOnNextPublishedMark = false;
                throw new IllegalStateException("the worker stopped before it recorded the publication");
            }
            Row row = rows.get(eventId);
            rows.put(eventId, new Row(RelayStatus.PUBLISHED, row.attempts() + 1, row.nextAttemptAt(), now, null));
        }

        @Override
        public void markRetry(UUID eventId, int attemptCount, Instant nextAttemptAt, String lastError) {
            rows.put(eventId, new Row(RelayStatus.PENDING, attemptCount, nextAttemptAt, null, lastError));
        }

        @Override
        public void markFailed(UUID eventId, RelayStatus status, int attemptCount, String lastError) {
            Row row = rows.get(eventId);
            rows.put(eventId, new Row(status, attemptCount, row.nextAttemptAt(), null, lastError));
        }
    }

    /** The schema check of the envelope. */
    static final class Validator implements EnvelopeValidator {
        private final Map<UUID, String> rejected = new HashMap<>();

        void reject(UUID eventId, String reason) {
            rejected.put(eventId, reason);
        }

        @Override
        public Optional<String> validate(OutboxEvent event) {
            return Optional.ofNullable(rejected.get(event.id()));
        }
    }

    /** The broker: records every delivery, including duplicates, and fails on demand. */
    static final class Publisher implements EventPublisher {
        private final Clock clock;
        private final List<UUID> delivered = new ArrayList<>();
        private final Set<UUID> failing = new HashSet<>();
        private boolean failAll;
        private PublishException failure;
        private int attempts;
        private Duration deliveryTime = Duration.ZERO;
        private String lastCorrelationId = "";

        Publisher(Clock clock) {
            this.clock = clock;
        }

        void failTransiently(boolean value) {
            this.failAll = value;
        }

        void failTransientlyFor(UUID eventId) {
            failing.add(eventId);
        }

        void failWith(PublishException failure) {
            this.failure = failure;
        }

        void takeEachDelivery(Duration time) {
            this.deliveryTime = time;
        }

        List<UUID> deliveredIds() {
            return List.copyOf(delivered);
        }

        List<UUID> deliveries() {
            return deliveredIds();
        }

        int attempts() {
            return attempts;
        }

        String lastRunCorrelationId() {
            return lastCorrelationId;
        }

        @Override
        public void publish(OutboxEvent event, String correlationId) {
            attempts++;
            lastCorrelationId = correlationId;
            if (failure != null) {
                throw failure;
            }
            if (failAll || failing.contains(event.id())) {
                throw PublishException.transientFailure("broker connection lost", null);
            }
            clock.advance(deliveryTime);
            delivered.add(event.id());
        }
    }
}
