package co.edu.corhuila.csp.worker.adapter.out.storage;

import co.edu.corhuila.csp.worker.application.port.out.PdfStorageException;
import co.edu.corhuila.csp.worker.application.port.out.TicketPdfStorage;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.ByteArrayInputStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Writes the ticket PDFs to the private bucket of the object storage, through its S3 API, with the writer account of the
 * worker (ADR-058): put and get on {@code tickets/*}, nothing else. A key is immutable, so an object that is already
 * there is kept and only its reference is returned. Nothing that could hold a credential goes into an error message.
 */
@Component
@ConditionalOnProperty(prefix = "worker.render-ticket-pdfs", name = "enabled", havingValue = "true")
public class S3TicketPdfStorage implements TicketPdfStorage {

    private final MinioClient client;
    private final String bucket;

    public S3TicketPdfStorage(
            @Value("${object-storage.url}") String url,
            @Value("${object-storage.access-key:}") String accessKey,
            @Value("${object-storage.secret-key:}") String secretKey,
            @Value("${object-storage.bucket:tickets}") String bucket) {
        if (accessKey.isBlank() || secretKey.isBlank()) {
            throw new IllegalStateException("OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY are required");
        }
        this.client = MinioClient.builder().endpoint(url).credentials(accessKey, secretKey).build();
        this.bucket = bucket;
    }

    @Override
    public String storeOnce(String key, byte[] pdf) {
        try {
            if (!exists(key)) {
                client.putObject(PutObjectArgs.builder().bucket(bucket).object(key)
                        .stream(new ByteArrayInputStream(pdf), pdf.length, -1).contentType("application/pdf").build());
            }
            return "s3://" + bucket + "/" + key;
        } catch (ErrorResponseException exception) {
            throw new PdfStorageException("the object storage refused the write: " + exception.errorResponse().code(),
                    exception);
        } catch (Exception exception) {
            throw new PdfStorageException("the object storage cannot be written: " + exception.getClass().getSimpleName(),
                    exception);
        }
    }

    private boolean exists(String key) throws Exception {
        try {
            client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
            return true;
        } catch (ErrorResponseException exception) {
            if ("NoSuchKey".equals(exception.errorResponse().code())) {
                return false;
            }
            throw exception;
        }
    }
}
