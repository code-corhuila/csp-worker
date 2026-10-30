package co.edu.corhuila.csp.worker.application.usecase;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * What the Ticketing service gives the worker to render the document of a ticket (ADR-057): the snapshot the ticket
 * keeps. {@code showtimeStartsAt} is null while Catalog does not exist, and {@code concessions} is empty when the
 * reservation has no concession order.
 */
public record TicketPdfData(
        UUID ticketId,
        UUID reservationId,
        String movieTitle,
        String roomName,
        Instant showtimeStartsAt,
        List<String> seatNumbers,
        List<Concession> concessions,
        String qrPayload
) {
    public record Concession(String name, int quantity, long unitPriceCents) {
    }
}
