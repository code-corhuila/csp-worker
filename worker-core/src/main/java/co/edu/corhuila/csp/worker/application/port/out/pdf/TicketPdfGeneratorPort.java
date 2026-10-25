package co.edu.corhuila.csp.worker.application.port.out.pdf;

import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;

public interface TicketPdfGeneratorPort {
    byte[] generateTicketPdf(TicketPdfData ticket);
}