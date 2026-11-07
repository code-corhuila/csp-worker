package co.edu.corhuila.csp.worker.application.port.out;

/** The object storage did not answer or refused a call. */
public class PdfStorageException extends RuntimeException {

    public PdfStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
