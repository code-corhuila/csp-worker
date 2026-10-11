package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.BookingExpireHoldsApi;
import co.edu.corhuila.csp.worker.application.port.out.ExpireHoldsResult;
import org.junit.jupiter.api.Test;

/**
 * The expiration sweep job: calls the booking api with a correlation id and reports the outcome.
 */
class ExpireHoldsJobTest {

    private final BookingExpireHoldsApi bookingApi = mock(BookingExpireHoldsApi.class);
    private final ExpireHoldsJob job = new ExpireHoldsJob(bookingApi);

    @Test
    void theJobCallsTheBookingApiWithACorrelationIdAndReportsTheOutcome() {
        when(bookingApi.expireHolds(anyString())).thenReturn(new ExpireHoldsResult(5, 2));

        JobResult result = job.run();

        assertEquals(5, result.processed());
        assertEquals(0, result.failed());
        verify(bookingApi).expireHolds(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void theJobReportsAFailureWhenTheBookingApiIsUnreachable() {
        when(bookingApi.expireHolds(anyString())).thenThrow(new RuntimeException("connection refused"));

        JobResult result = job.run();

        assertEquals(0, result.processed());
        assertEquals(1, result.failed());
    }

    @Test
    void theJobHasAName() {
        assertEquals("expire-holds", job.name());
    }
}
