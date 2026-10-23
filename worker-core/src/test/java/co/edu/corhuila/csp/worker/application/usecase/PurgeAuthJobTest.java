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
import co.edu.corhuila.csp.worker.application.port.out.AuthMaintenanceApi;
import co.edu.corhuila.csp.worker.application.port.out.PurgeResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The purge job: drains the purge of the identity service in batches, bounded by time and by calls. */
class PurgeAuthJobTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private final AuthMaintenanceApi authApi = mock(AuthMaintenanceApi.class);

    @Test
    void aRunCallsAgainWhileTheServiceHasMoreAndAddsUpWhatWasProcessed() {
        when(authApi.purge(anyString(), eq(200)))
                .thenReturn(new PurgeResult(200, true), new PurgeResult(200, true), new PurgeResult(70, false));

        JobResult result = job(200).run();

        assertEquals(new JobResult(470, 0), result);
        verify(authApi, times(3)).purge(anyString(), eq(200));
    }

    @Test
    void everyCallOfARunCarriesTheSameCorrelationId() {
        when(authApi.purge(anyString(), anyInt()))
                .thenReturn(new PurgeResult(5, true), new PurgeResult(5, false));

        job(5).run();

        ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
        verify(authApi, times(2)).purge(ids.capture(), anyInt());
        assertEquals(ids.getAllValues().get(0), ids.getAllValues().get(1));
    }

    @Test
    void aFailingCallEndsTheRunKeepingWhatWasAlreadyProcessed() {
        when(authApi.purge(anyString(), anyInt()))
                .thenReturn(new PurgeResult(10, true))
                .thenThrow(new IllegalStateException("the identity service answered 503"));

        assertEquals(new JobResult(10, 1), job(10).run());
    }

    @Test
    void aRunWhoseFirstCallFailsReportsOneFailure() {
        when(authApi.purge(anyString(), anyInt())).thenThrow(new RuntimeException("connection refused"));

        assertEquals(new JobResult(0, 1), job(10).run());
    }

    @Test
    void aRunStopsWhenItsTimeIsUp() {
        MutableClock clock = new MutableClock();
        when(authApi.purge(anyString(), anyInt())).thenAnswer(call -> {
            clock.advance(Duration.ofSeconds(30));
            return new PurgeResult(10, true);
        });

        JobResult result = new PurgeAuthJob(authApi, 10, TIMEOUT, clock).run();

        assertEquals(new JobResult(20, 0), result);
        verify(authApi, times(2)).purge(anyString(), anyInt());
    }

    @Test
    void aRunNeverMakesMoreCallsThanTheCap() {
        when(authApi.purge(anyString(), anyInt())).thenReturn(new PurgeResult(1, true));

        JobResult result = job(1).run();

        assertEquals(PurgeAuthJob.MAX_CALLS_PER_RUN, result.processed());
        verify(authApi, times(PurgeAuthJob.MAX_CALLS_PER_RUN)).purge(anyString(), anyInt());
    }

    @Test
    void aRunStopsWhenTheServiceMakesNoProgress() {
        when(authApi.purge(anyString(), anyInt())).thenReturn(new PurgeResult(0, true));

        assertEquals(new JobResult(0, 0), job(10).run());
        verify(authApi, times(1)).purge(anyString(), anyInt());
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
                () -> new PurgeAuthJob(authApi, 10, Duration.ZERO, Clock.systemUTC()));
    }

    @Test
    void theJobHasAName() {
        assertEquals("purge-auth", job(10).name());
    }

    private PurgeAuthJob job(int batchSize) {
        return new PurgeAuthJob(authApi, batchSize, TIMEOUT, Clock.systemUTC());
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
