package co.edu.corhuila.csp.worker.application.usecase;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record TicketPdfData(
        UUID ticketId,
        UUID reservationId,
        UUID showtimeId,
        UUID userContactId,
        String movieTitleSnapshot,
        String roomNameSnapshot,
        Instant showtimeStartsAt,
        List<String> seatLabelsSnapshot,
        List<ConcessionItem> concessionsSnapshot,
        String qrPayload
) {
    public record ConcessionItem(
            String name,
            int quantity,
            long unitPriceCents
    ) {}
}