package co.edu.corhuila.csp.worker.adapter.out.pdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The document of a ticket: a real PDF well under the 1 MiB the job accepts, with or without start time and concessions. */
class ITextPdfGeneratorAdapterTest {

    private final ITextPdfGeneratorAdapter generator = new ITextPdfGeneratorAdapter();

    private static TicketPdfData ticket(Instant startsAt, List<TicketPdfData.Concession> concessions) {
        return new TicketPdfData(UUID.randomUUID(), UUID.randomUUID(), "Dune", "Room 1", startsAt, List.of("A1", "A2"),
                concessions, "11111111-1111-4111-8111-111111111111");
    }

    @Test
    void aTicketWithAStartTimeAndConcessionsIsAPdfUnderOneMiB() {
        byte[] pdf = generator.generateTicketPdf(ticket(Instant.parse("2026-11-01T20:00:00Z"),
                List.of(new TicketPdfData.Concession("Popcorn", 2, 12000))));

        assertEquals("%PDF", new String(pdf, 0, 4, StandardCharsets.US_ASCII));
        assertTrue(pdf.length < 1024 * 1024, "the pdf is " + pdf.length + " bytes");
    }

    @Test
    void aTicketWithoutStartTimeNorConcessionsIsStillAPdf() {
        byte[] pdf = generator.generateTicketPdf(ticket(null, List.of()));

        assertEquals("%PDF", new String(pdf, 0, 4, StandardCharsets.US_ASCII));
    }
}
