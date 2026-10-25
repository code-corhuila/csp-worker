package co.edu.corhuila.csp.worker.adapter.out.storage;

import co.edu.corhuila.csp.worker.application.port.out.storage.ObjectStoragePort;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;

@Component
public class MinioObjectStorageAdapter implements ObjectStoragePort {
    private final MinioClient client;
    private final String bucket;

    public MinioObjectStorageAdapter(@Value("${object-storage.url}") String url,
                                     @Value("${object-storage.access-key}") String ak,
                                     @Value("${object-storage.secret-key}") String sk,
                                     @Value("${object-storage.bucket:tickets}") String bucket) {
        this.bucket = bucket;
        this.client = MinioClient.builder().endpoint(url).credentials(ak, sk).build();
        try { if (!client.bucketExists(io.minio.BucketExistsArgs.builder().bucket(bucket).build())) client.makeBucket(io.minio.MakeBucketArgs.builder().bucket(bucket).build()); }
        catch (Exception e) { /* ignore */ }
    }

    @Override public String uploadPdf(String key, byte[] pdf, String contentType) {
        try (var in = new ByteArrayInputStream(pdf)) {
            client.putObject(PutObjectArgs.builder().bucket(bucket).object(key).stream(new ByteArrayInputStream(pdf), pdf.length, -1).contentType(contentType).build());
            return "s3://" + bucket + "/" + key;
        } catch (Exception e) { throw new RuntimeException("S3 upload failed", e); }
    }
}