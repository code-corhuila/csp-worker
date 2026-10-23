package co.edu.corhuila.csp.worker.application.usecase;

import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.OrdersApiException;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue.Delivery;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Settles the concession order of a reservation when Booking says the reservation was confirmed or expired
 * (HU-CONCESSIONS-004, ADR-045): it takes the events from the queues of Concessions and asks the Concessions
 * API to confirm or release the order. A message is acknowledged only after the service accepted it, so a
 * crash in between repeats the call, which the service answers without effect (Norma 5.7, rule 1). Every call of
 * a run carries the same correlation id (Norma 5.7.3).
 *
 * <p>Each message is settled on its own: one the service refuses for good goes to the dead-letter queue and the
 * batch continues; when the service is down the message goes back and the run stops, because every other call
 * would fail the same way. A run is bounded by its batch size and its time (Norma 5.7).
 */
public class ReservationEventsJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(ReservationEventsJob.class);

    private final ReservationEventQueue queue;
    private final ConcessionsOrdersApi api;
    private final int batchSize;
    private final Duration runTimeout;
    private final Clock clock;

    public ReservationEventsJob(ReservationEventQueue queue, ConcessionsOrdersApi api, int batchSize,
            Duration runTimeout, Clock clock) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("the batch size of the reservation events must be positive");
        }
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("the run timeout of the reservation events must be positive");
        }
        this.queue = queue;
        this.api = api;
        this.batchSize = batchSize;
        this.runTimeout = runTimeout;
        this.clock = clock;
    }

    @Override
    public JobResult run() {
        String correlationId = UUID.randomUUID().toString();
        Instant deadline = clock.instant().plus(runTimeout);
        int processed = 0;
        int failed = 0;
        try {
            while (processed + failed < batchSize && clock.instant().isBefore(deadline)) {
                Optional<Delivery> next = queue.next();
                if (next.isEmpty()) {
                    break;
                }
                Delivery delivery = next.get();
                Optional<ReservationEvent> event = delivery.event();
                if (event.isEmpty()) {
                    log.error("a message of the reservation queues is not a reservation event: sent to the dead-letter queue");
                    delivery.reject();
                    failed++;
                    continue;
                }
                try {
                    settle(event.get(), correlationId);
                    delivery.ack();
                    processed++;
                } catch (OrdersApiException exception) {
                    failed++;
                    if (exception.retryable()) {
                        log.warn("event {} left for a later run: {}", event.get().eventId(), exception.getMessage());
                        delivery.requeue();
                        break;
                    }
                    log.error("event {} refused by the concessions service: {}", event.get().eventId(),
                            exception.getMessage());
                    delivery.reject();
                }
            }
        } catch (RuntimeException exception) {
            log.error("reservation events run failed: processed={}, correlationId={}", processed, correlationId,
                    exception);
            return new JobResult(processed, failed + 1);
        }
        if (processed + failed > 0) {
            log.info("reservation events run: processed={}, failed={}, correlationId={}", processed, failed,
                    correlationId);
        }
        return new JobResult(processed, failed);
    }

    private void settle(ReservationEvent event, String correlationId) {
        switch (event.type()) {
            case BOOKING_CONFIRMED -> api.confirmByReservation(event.reservationId(), correlationId);
            case RESERVATION_EXPIRED -> api.releaseByReservation(event.reservationId(), correlationId);
        }
    }

    @Override
    public String name() {
        return "concessions-reservation-events";
    }
}
