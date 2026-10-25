package co.edu.corhuila.csp.worker.application.port.out.storage;

public interface ObjectStoragePort {
    String uploadPdf(String objectKey, byte[] pdfBytes, String contentType);
}