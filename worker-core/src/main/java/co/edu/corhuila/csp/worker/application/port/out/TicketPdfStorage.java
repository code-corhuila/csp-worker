package co.edu.corhuila.csp.worker.application.port.out;

/** The private bucket of the ticket PDFs, as the worker writes it with its own account (ADR-058). */
public interface TicketPdfStorage {

    /**
     * Writes the PDF under the key unless an object is already there: a key is immutable, so a retry never creates a
     * second version of a ticket.
     *
     * @return the reference of the object, which is the same whether it was written now or before
     * @throws PdfStorageException when the storage did not accept the call
     */
    String storeOnce(String key, byte[] pdf);
}
