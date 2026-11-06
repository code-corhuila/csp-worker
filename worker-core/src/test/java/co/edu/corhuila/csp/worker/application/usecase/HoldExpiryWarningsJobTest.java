package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingHoldExpiryWarningsApi;
import org.junit.jupiter.api.Test;

/** The warning of the holds about to expire (HU-TICKETING-005): one bounded call per run, a failure never escapes. */
class HoldExpiryWarningsJobTest {

    private final TicketingHoldExpiryWarningsApi ticketing = mock(TicketingHoldExpiryWarningsApi.class);
    private final HoldExpiryWarningsJob job = new HoldExpiryWarningsJob(ticketing);

    @Test
    void aRunCallsTheServiceOnceWithTheLimitAndReportsHowManyWereWarned() {
        when(ticketing.warn(anyString(), anyInt())).thenReturn(7);

        JobResult result = job.run();

        assertEquals(new JobResult(7, 0), result);
        verify(ticketing, times(1)).warn(anyString(), eq(100));
    }

    @Test
    void aRefusedCallIsReportedAsAFailedRunAndDoesNotThrow() {
        when(ticketing.warn(anyString(), anyInt())).thenThrow(new TicketingApiException("answered 500"));

        assertEquals(new JobResult(0, 1), job.run());
    }

    @Test
    void aRunAfterAFailureCallsTheServiceAgainWithoutAccumulating() {
        when(ticketing.warn(anyString(), anyInt())).thenThrow(new TicketingApiException("timeout")).thenReturn(3);

        assertEquals(new JobResult(0, 1), job.run());
        assertEquals(new JobResult(3, 0), job.run());
        verify(ticketing, times(2)).warn(anyString(), eq(100));
    }

    @Test
    void theJobHasAName() {
        assertEquals("hold-expiry-warnings", job.name());
    }
}
