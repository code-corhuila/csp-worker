package co.edu.corhuila.csp.worker.application.port.out.ticketing;

public interface TicketingApi {
    void submitPdfUrl(String ticketId, String pdfUrl);
}