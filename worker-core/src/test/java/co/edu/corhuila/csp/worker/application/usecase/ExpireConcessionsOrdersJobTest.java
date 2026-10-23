package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.ConcessionsOrdersApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireOrdersResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The sweep of the orders whose time ran out (ADR-045): drains the concessions service in batches, bounded by time and by calls. */
class ExpireConcessionsOrdersJobTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private final ConcessionsOrdersApi ordersApi = mock(ConcessionsOrdersApi.class);

    @Test
    void aRunCallsAgainWhileTheServiceHasMoreAndAddsUpWhatWasProcessed() {
        when(ordersApi.expireOrders(anyString(), eq(200)))
                .thenReturn(new ExpireOrdersResult(200, true), new ExpireOrdersResult(200, true), new ExpireOrdersResult(70, false));

        JobResult result = job(200).run();

        assertEquals(new JobResult(470, 0), result);
        verify(ordersApi, times(3)).expireOrders(anyString(), eq(200));
    }

    @Test
    void everyCallOfARunCarriesTheSameCorrelationId() {
        when(ordersApi.expireOrders(anyString(), anyInt()))
                .thenReturn(new ExpireOrdersResult(5, true), new ExpireOrdersResult(5, false));

        job(5).run();

        ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
        verify(ordersApi, times(2)).expireOrders(ids.capture(), anyInt());
        assertEquals(ids.getAllValues().get(0), ids.getAllValues().get(1));
    }

    @Test
    void aFailingCallEndsTheRunKeepingWhatWasAlreadyProcessed() {
        when(ordersApi.expireOrders(anyString(), anyInt()))
                .thenReturn(new ExpireOrdersResult(10, true))
                .thenThrow(new IllegalStateException("the concessions service answered 503"));

        assertEquals(new JobResult(10, 1), job(10).run());
    }

    @Test
    void aRunWhoseFirstCallFailsReportsOneFailure() {
        when(ordersApi.expireOrders(anyString(), anyInt())).thenThrow(new RuntimeException("connection refused"));

        assertEquals(new JobResult(0, 1), job(10).run());
    }

    @Test
    void aRunStopsWhenItsTimeIsUp() {
        MutableClock clock = new MutableClock();
        when(ordersApi.expireOrders(anyString(), anyInt())).thenAnswer(call -> {
            clock.advance(Duration.ofSeconds(30));
            return new ExpireOrdersResult(10, true);
        });

        JobResult result = new ExpireConcessionsOrdersJob(ordersApi, 10, TIMEOUT, clock).run();

        assertEquals(new JobResult(20, 0), result);
        verify(ordersApi, times(2)).expireOrders(anyString(), anyInt());
    }

    @Test
    void aRunNeverMakesMoreCallsThanTheCap() {
        when(ordersApi.expireOrders(anyString(), anyInt())).thenReturn(new ExpireOrdersResult(1, true));

        JobResult result = job(1).run();

        assertEquals(DrainLoop.MAX_CALLS_PER_RUN, result.processed());
        verify(ordersApi, times(DrainLoop.MAX_CALLS_PER_RUN)).expireOrders(anyString(), anyInt());
    }

    @Test
    void aRunStopsWhenTheServiceMakesNoProgress() {
        when(ordersApi.expireOrders(anyString(), anyInt())).thenReturn(new ExpireOrdersResult(0, true));

        assertEquals(new JobResult(0, 0), job(10).run());
        verify(ordersApi, times(1)).expireOrders(anyString(), anyInt());
    }

    @Test
    void theBatchSizeMustBeBetween1AndTheContractMaximum() {
        assertThrows(IllegalArgumentException.class, () -> job(0));
        assertThrows(IllegalArgumentException.class, () -> job(501));
        job(1);
        job(500);
    }

    @Test
    void theRunTimeoutMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new ExpireConcessionsOrdersJob(ordersApi, 10, Duration.ZERO, Clock.systemUTC()));
    }

    @Test
    void theJobHasAName() {
        assertEquals("expire-concessions-orders", job(10).name());
    }

    private ExpireConcessionsOrdersJob job(int batchSize) {
        return new ExpireConcessionsOrdersJob(ordersApi, batchSize, TIMEOUT, Clock.systemUTC());
    }

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-14T00:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
