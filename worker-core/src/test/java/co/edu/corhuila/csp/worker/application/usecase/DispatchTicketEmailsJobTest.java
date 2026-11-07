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
import co.edu.corhuila.csp.worker.application.port.out.TicketingDispatchApi;
import co.edu.corhuila.csp.worker.application.port.out.TicketingDispatchApi.Result;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The dispatch of the ticket emails (ADR-057): drains the Ticketing service in batches, bounded by time and by calls. */
class DispatchTicketEmailsJobTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private final TicketingDispatchApi ticketing = mock(TicketingDispatchApi.class);

    private DispatchTicketEmailsJob job(int batchSize) {
        return new DispatchTicketEmailsJob(ticketing, batchSize, TIMEOUT, Clock.systemUTC());
    }

    @Test
    void aFullBatchMakesTheRunCallAgainAndEverythingHandledIsAdded() {
        when(ticketing.dispatch(anyString(), eq(10)))
                .thenReturn(new Result(8, 1, 1), new Result(10, 0, 0), new Result(2, 1, 0));

        JobResult result = job(10).run();

        assertEquals(new JobResult(23, 0), result);
        verify(ticketing, times(3)).dispatch(anyString(), eq(10));
    }

    @Test
    void aBatchSmallerThanTheLimitEndsTheRun() {
        when(ticketing.dispatch(anyString(), anyInt())).thenReturn(new Result(3, 0, 0));

        assertEquals(new JobResult(3, 0), job(10).run());
        verify(ticketing, times(1)).dispatch(anyString(), anyInt());
    }

    @Test
    void nothingDueEndsTheRunWithNothingProcessed() {
        when(ticketing.dispatch(anyString(), anyInt())).thenReturn(new Result(0, 0, 0));

        assertEquals(new JobResult(0, 0), job(10).run());
    }

    @Test
    void everyCallOfARunCarriesTheSameCorrelationId() {
        when(ticketing.dispatch(anyString(), anyInt())).thenReturn(new Result(5, 0, 0), new Result(1, 0, 0));

        job(5).run();

        ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
        verify(ticketing, times(2)).dispatch(ids.capture(), anyInt());
        assertEquals(ids.getAllValues().get(0), ids.getAllValues().get(1));
    }

    @Test
    void aFailingCallEndsTheRunKeepingWhatWasAlreadyHandled() {
        when(ticketing.dispatch(anyString(), anyInt()))
                .thenReturn(new Result(10, 0, 0))
                .thenThrow(new IllegalStateException("the ticketing service answered 503"));

        assertEquals(new JobResult(10, 1), job(10).run());
    }

    @Test
    void theBatchSizeMustBeBetweenOneAndOneHundredAndTheTimeoutPositive() {
        assertThrows(IllegalArgumentException.class, () -> job(0));
        assertThrows(IllegalArgumentException.class, () -> job(101));
        assertThrows(IllegalArgumentException.class,
                () -> new DispatchTicketEmailsJob(ticketing, 10, Duration.ZERO, Clock.systemUTC()));
    }

    @Test
    void theJobHasItsOwnName() {
        assertEquals("dispatch-ticket-emails", job(10).name());
    }
}
