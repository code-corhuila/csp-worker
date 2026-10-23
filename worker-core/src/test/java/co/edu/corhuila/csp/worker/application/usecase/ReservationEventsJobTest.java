package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.OrdersApiException;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEvent.Type;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue;
import co.edu.corhuila.csp.worker.application.port.out.ReservationEventQueue.Delivery;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * The reservation events job (HU-CONCESSIONS-004, ADR-045): takes BookingConfirmed and ReservationExpired
 * from the queues of Concessions and asks the Concessions API to confirm or release the order of the
 * reservation. A message is acknowledged only after the API accepted it.
 */
class ReservationEventsJobTest {

    private static final String RESERVATION = "44444444-4444-4444-8444-444444444444";

    private final ConcessionsOrdersApi api = mock(ConcessionsOrdersApi.class);
    private final Deque<FakeDelivery> pending = new ArrayDeque<>();
    private final ReservationEventQueue queue = () -> Optional.ofNullable(pending.poll());
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-22T12:00:00Z"), ZoneOffset.UTC);

    private ReservationEventsJob job(int batchSize) {
        return new ReservationEventsJob(queue, api, batchSize, Duration.ofSeconds(20), clock);
    }

    private FakeDelivery deliver(Type type) {
        FakeDelivery delivery = new FakeDelivery(Optional.of(new ReservationEvent("event-" + pending.size(), type, RESERVATION)));
        pending.add(delivery);
        return delivery;
    }

    @Test
    void aConfirmedReservationConfirmsItsOrderAndTheMessageIsAcknowledged() {
        FakeDelivery delivery = deliver(Type.BOOKING_CONFIRMED);

        JobResult result = job(10).run();

        assertEquals(new JobResult(1, 0), result);
        verify(api).confirmByReservation(org.mockito.ArgumentMatchers.eq(RESERVATION), anyString());
        verify(api, never()).releaseByReservation(anyString(), anyString());
        assertEquals("ack", delivery.outcome);
    }

    @Test
    void anExpiredReservationReleasesItsOrderAndTheMessageIsAcknowledged() {
        FakeDelivery delivery = deliver(Type.RESERVATION_EXPIRED);

        JobResult result = job(10).run();

        assertEquals(new JobResult(1, 0), result);
        verify(api).releaseByReservation(org.mockito.ArgumentMatchers.eq(RESERVATION), anyString());
        assertEquals("ack", delivery.outcome);
    }

    @Test
    void everyCallOfARunCarriesTheSameCorrelationId() {
        deliver(Type.BOOKING_CONFIRMED);
        deliver(Type.RESERVATION_EXPIRED);

        job(10).run();

        ArgumentCaptor<String> confirm = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> release = ArgumentCaptor.forClass(String.class);
        verify(api).confirmByReservation(anyString(), confirm.capture());
        verify(api).releaseByReservation(anyString(), release.capture());
        assertEquals(confirm.getValue(), release.getValue());
    }

    @Test
    void aMessageTheServiceRefusesForGoodGoesToTheDeadLetterQueueAndTheBatchContinues() {
        FakeDelivery refused = deliver(Type.BOOKING_CONFIRMED);
        FakeDelivery fine = deliver(Type.RESERVATION_EXPIRED);
        doThrow(new OrdersApiException("the order is CANCELLED and cannot be changed", false))
                .when(api).confirmByReservation(anyString(), anyString());

        JobResult result = job(10).run();

        assertEquals(new JobResult(1, 1), result);
        assertEquals("reject", refused.outcome);
        assertEquals("ack", fine.outcome);
    }

    @Test
    void aMessageThatCannotBeReadGoesToTheDeadLetterQueueWithoutCallingTheService() {
        FakeDelivery malformed = new FakeDelivery(Optional.empty());
        pending.add(malformed);

        JobResult result = job(10).run();

        assertEquals(new JobResult(0, 1), result);
        assertEquals("reject", malformed.outcome);
        verify(api, never()).confirmByReservation(anyString(), anyString());
        verify(api, never()).releaseByReservation(anyString(), anyString());
    }

    @Test
    void whenTheServiceIsDownTheMessageStaysInTheQueueAndTheRunStops() {
        FakeDelivery first = deliver(Type.BOOKING_CONFIRMED);
        FakeDelivery second = deliver(Type.BOOKING_CONFIRMED);
        doThrow(new OrdersApiException("the concessions service is unreachable", true))
                .when(api).confirmByReservation(anyString(), anyString());

        JobResult result = job(10).run();

        assertEquals(new JobResult(0, 1), result);
        assertEquals("requeue", first.outcome);
        assertEquals(null, second.outcome);
        verify(api, times(1)).confirmByReservation(anyString(), anyString());
    }

    @Test
    void aRunTakesAtMostTheBatchSize() {
        for (int i = 0; i < 5; i++) {
            deliver(Type.BOOKING_CONFIRMED);
        }

        JobResult result = job(3).run();

        assertEquals(new JobResult(3, 0), result);
        assertEquals(2, pending.size());
    }

    @Test
    void anEmptyQueueIsAnEmptyRun() {
        assertEquals(new JobResult(0, 0), job(10).run());
    }

    @Test
    void aRunStopsWhenItsTimeIsOver() {
        deliver(Type.BOOKING_CONFIRMED);
        Instant[] now = {Instant.parse("2026-10-22T12:00:00Z")};
        Clock moving = new Clock() {
            @Override
            public java.time.ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                Instant current = now[0];
                now[0] = now[0].plusSeconds(30);
                return current;
            }
        };

        JobResult result = new ReservationEventsJob(queue, api, 10, Duration.ofSeconds(20), moving).run();

        assertEquals(new JobResult(0, 0), result);
        assertEquals(1, pending.size());
    }

    @Test
    void theBatchSizeAndTheTimeMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> job(0));
        assertThrows(IllegalArgumentException.class,
                () -> new ReservationEventsJob(queue, api, 10, Duration.ZERO, clock));
    }

    @Test
    void whenTheQueueItselfFailsTheRunReportsOneFailure() {
        ReservationEventQueue broken = mock(ReservationEventQueue.class);
        when(broken.next()).thenThrow(new IllegalStateException("the broker is unreachable"));

        JobResult result = new ReservationEventsJob(broken, api, 10, Duration.ofSeconds(20), clock).run();

        assertEquals(new JobResult(0, 1), result);
    }

    private static final class FakeDelivery implements Delivery {
        private final Optional<ReservationEvent> event;
        String outcome;

        FakeDelivery(Optional<ReservationEvent> event) {
            this.event = event;
        }

        @Override
        public Optional<ReservationEvent> event() {
            return event;
        }

        @Override
        public void ack() {
            outcome = "ack";
        }

        @Override
        public void reject() {
            outcome = "reject";
        }

        @Override
        public void requeue() {
            outcome = "requeue";
        }
    }
}
