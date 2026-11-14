package co.edu.corhuila.csp.worker.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.port.out.PdfStorageException;
import co.edu.corhuila.csp.worker.application.port.out.TicketPdfStorage;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingPdfApi;
import co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The rendering of the ticket PDFs (ADR-057, ADR-058): list, render, store once under the key, report the reference. */
class RenderTicketPdfsJobTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final byte[] PDF = {'%', 'P', 'D', 'F'};

    private final TicketingPdfApi ticketing = mock(TicketingPdfApi.class);
    private final TicketPdfGeneratorPort generator = mock(TicketPdfGeneratorPort.class);
    private final TicketPdfStorage storage = mock(TicketPdfStorage.class);

    private RenderTicketPdfsJob job(int batchSize) {
        return new RenderTicketPdfsJob(ticketing, generator, storage, batchSize, TIMEOUT, Clock.systemUTC());
    }

    private static TicketPdfData ticket() {
        return new TicketPdfData(UUID.randomUUID(), UUID.randomUUID(), "Dune", "Room 1", Instant.parse("2026-11-01T20:00:00Z"),
                List.of("A1", "A2"), List.of(new TicketPdfData.Concession("Popcorn", 1, 12000)), "QR-PAYLOAD");
    }

    private void storesUnderTheReference() {
        when(storage.storeOnce(anyString(), any())).thenAnswer(call -> "s3://tickets/" + call.getArgument(0));
        when(generator.generateTicketPdf(any())).thenReturn(PDF);
    }

    @Test
    void aTicketIsRenderedStoredUnderItsKeyAndReportedWithTheReference() {
        TicketPdfData ticket = ticket();
        storesUnderTheReference();
        when(ticketing.awaitingPdf(anyString(), eq(10))).thenReturn(List.of(ticket));

        JobResult result = job(10).run();

        String key = "tickets/" + ticket.reservationId() + "/" + ticket.ticketId() + ".pdf";
        assertEquals(new JobResult(1, 0), result);
        verify(generator).generateTicketPdf(ticket);
        verify(storage).storeOnce(key, PDF);
        verify(ticketing).registerPdf(anyString(), eq(ticket.ticketId()), eq("s3://tickets/" + key));
    }

    @Test
    void noTicketAwaitingEndsTheRunWithoutTouchingTheStorage() {
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of());

        assertEquals(new JobResult(0, 0), job(10).run());
        verify(storage, never()).storeOnce(anyString(), any());
    }

    @Test
    void aFullBatchMakesTheRunAskAgainAndAShortOneEndsIt() {
        storesUnderTheReference();
        when(ticketing.awaitingPdf(anyString(), eq(2)))
                .thenReturn(List.of(ticket(), ticket()), List.of(ticket()));

        assertEquals(new JobResult(3, 0), job(2).run());
        verify(ticketing, times(2)).awaitingPdf(anyString(), eq(2));
    }

    @Test
    void everyCallOfARunCarriesTheSameCorrelationId() {
        TicketPdfData ticket = ticket();
        storesUnderTheReference();
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(ticket));

        job(10).run();

        ArgumentCaptor<String> listed = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> reported = ArgumentCaptor.forClass(String.class);
        verify(ticketing).awaitingPdf(listed.capture(), anyInt());
        verify(ticketing).registerPdf(reported.capture(), any(), anyString());
        assertEquals(listed.getValue(), reported.getValue());
    }

    @Test
    void aPdfOverOneMiBIsNeitherStoredNorReportedAndTheOthersGoOn() {
        TicketPdfData big = ticket();
        TicketPdfData small = ticket();
        storesUnderTheReference();
        when(generator.generateTicketPdf(big)).thenReturn(new byte[RenderTicketPdfsJob.MAX_PDF_BYTES + 1]);
        when(generator.generateTicketPdf(small)).thenReturn(PDF);
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(big, small));

        assertEquals(new JobResult(1, 1), job(10).run());
        verify(storage, never()).storeOnce(eq(RenderTicketPdfsJob.keyOf(big)), any());
        verify(ticketing, never()).registerPdf(anyString(), eq(big.ticketId()), anyString());
        verify(ticketing).registerPdf(anyString(), eq(small.ticketId()), anyString());
    }

    @Test
    void aPdfOfExactlyOneMiBIsAccepted() {
        TicketPdfData ticket = ticket();
        storesUnderTheReference();
        when(generator.generateTicketPdf(ticket)).thenReturn(new byte[RenderTicketPdfsJob.MAX_PDF_BYTES]);
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(ticket));

        assertEquals(new JobResult(1, 0), job(10).run());
    }

    @Test
    void aTicketWhoseRenderingFailsDoesNotStopTheBatch() {
        TicketPdfData broken = ticket();
        TicketPdfData fine = ticket();
        storesUnderTheReference();
        when(generator.generateTicketPdf(broken)).thenThrow(new IllegalStateException("PDF gen failed"));
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(broken, fine));

        assertEquals(new JobResult(1, 1), job(10).run());
        verify(ticketing).registerPdf(anyString(), eq(fine.ticketId()), anyString());
    }

    @Test
    void aStorageFailureIsReportedAsAFailureOfThatTicketOnly() {
        TicketPdfData first = ticket();
        TicketPdfData second = ticket();
        when(generator.generateTicketPdf(any())).thenReturn(PDF);
        when(storage.storeOnce(eq(RenderTicketPdfsJob.keyOf(first)), any()))
                .thenThrow(new PdfStorageException("the storage is down", null));
        when(storage.storeOnce(eq(RenderTicketPdfsJob.keyOf(second)), any())).thenReturn("s3://tickets/second");
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(first, second));

        assertEquals(new JobResult(1, 1), job(10).run());
        verify(ticketing, never()).registerPdf(anyString(), eq(first.ticketId()), anyString());
    }

    @Test
    void aRefusedReportLeavesTheStoredPdfAndCountsAsAFailure() {
        TicketPdfData ticket = ticket();
        storesUnderTheReference();
        doThrow(new TicketingApiException("the ticketing service answered 503"))
                .when(ticketing).registerPdf(anyString(), eq(ticket.ticketId()), anyString());
        when(ticketing.awaitingPdf(anyString(), anyInt())).thenReturn(List.of(ticket));

        assertEquals(new JobResult(0, 1), job(10).run());
        verify(storage).storeOnce(eq(RenderTicketPdfsJob.keyOf(ticket)), any());
    }

    @Test
    void aTicketThatFailedIsNotTriedAgainInTheSameRunWhenTheListRepeatsIt() {
        TicketPdfData broken = ticket();
        TicketPdfData a = ticket();
        TicketPdfData b = ticket();
        storesUnderTheReference();
        when(generator.generateTicketPdf(broken)).thenThrow(new IllegalStateException("PDF gen failed"));
        when(ticketing.awaitingPdf(anyString(), eq(2)))
                .thenReturn(List.of(broken, a), List.of(broken, b), List.of(broken));

        assertEquals(new JobResult(2, 1), job(2).run());
        verify(generator, times(1)).generateTicketPdf(broken);
    }

    @Test
    void aFailingListEndsTheRunKeepingWhatWasAlreadyRendered() {
        storesUnderTheReference();
        when(ticketing.awaitingPdf(anyString(), anyInt()))
                .thenReturn(List.of(ticket(), ticket()))
                .thenThrow(new TicketingApiException("the ticketing service answered 503"));

        assertEquals(new JobResult(2, 1), job(2).run());
    }

    @Test
    void theRunStopsWhenItsTimeIsOut() {
        Instant start = Instant.parse("2026-11-01T10:00:00Z");
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(start, start.plus(TIMEOUT).plusSeconds(1));
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);

        RenderTicketPdfsJob timed = new RenderTicketPdfsJob(ticketing, generator, storage, 10, TIMEOUT, clock);

        assertEquals(new JobResult(0, 0), timed.run());
        verify(ticketing, never()).awaitingPdf(anyString(), anyInt());
    }

    @Test
    void theBatchSizeMustBeBetweenOneAndOneHundredAndTheTimeoutPositive() {
        assertThrows(IllegalArgumentException.class, () -> job(0));
        assertThrows(IllegalArgumentException.class, () -> job(101));
        assertThrows(IllegalArgumentException.class,
                () -> new RenderTicketPdfsJob(ticketing, generator, storage, 10, Duration.ZERO, Clock.systemUTC()));
    }

    @Test
    void theJobIsNamedForTheLogsAndTheMetrics() {
        assertEquals("render-ticket-pdfs", job(10).name());
    }
}
