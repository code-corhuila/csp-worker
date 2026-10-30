package co.edu.corhuila.csp.worker.application.port.out;

import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import java.util.List;
import java.util.UUID;

/**
 * The Ticketing service as seen by the worker for the document of a ticket (ADR-057): which tickets still need their
 * PDF, and the report that a PDF exists. The worker never touches the ticketing schema.
 */
public interface TicketingPdfApi {

    /**
     * Calls {@code GET /internal/tickets/awaiting-pdf}: the tickets without a PDF, oldest first, once old enough for
     * the concession order to have arrived.
     *
     * @param limit how many tickets, from 1 to 100
     * @throws TicketingApiException when the service did not accept it
     */
    List<TicketPdfData> awaitingPdf(String correlationId, int limit);

    /**
     * Calls {@code POST /internal/tickets/{ticketId}/pdf}: the service records the reference of the stored PDF, which
     * cannot change afterwards, and then writes {@code TicketIssued} to its outbox.
     *
     * @throws TicketingApiException when the service did not accept it
     */
    void registerPdf(String correlationId, UUID ticketId, String pdfUrl);
}
