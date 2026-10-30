package co.edu.corhuila.csp.worker.adapter.out.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.application.port.out.PdfStorageException;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.SetBucketVersioningArgs;
import io.minio.messages.Item;
import io.minio.messages.VersioningConfiguration;
import java.io.InputStream;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The writer of the ticket PDFs against a real S3-compatible storage (ADR-058): the tests run when {@code TEST_S3_URL},
 * {@code TEST_S3_ACCESS_KEY} and {@code TEST_S3_SECRET_KEY} are set (RustFS in the CI) and are skipped otherwise. The
 * bucket is versioned like the one of {@code csp-infra-rustfs}, so a second write of a key would show as a second version.
 */
@EnabledIfEnvironmentVariable(named = "TEST_S3_URL", matches = ".+")
class S3TicketPdfStorageTest {

    private static final String BUCKET = "tickets";

    private static String url;
    private static String accessKey;
    private static String secretKey;
    private static MinioClient admin;

    @BeforeAll
    static void createTheVersionedBucket() throws Exception {
        url = System.getenv("TEST_S3_URL");
        accessKey = System.getenv("TEST_S3_ACCESS_KEY");
        secretKey = System.getenv("TEST_S3_SECRET_KEY");
        admin = MinioClient.builder().endpoint(url).credentials(accessKey, secretKey).build();
        if (!admin.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
            admin.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }
        admin.setBucketVersioning(SetBucketVersioningArgs.builder().bucket(BUCKET)
                .config(new VersioningConfiguration(VersioningConfiguration.Status.ENABLED, null)).build());
    }

    private static S3TicketPdfStorage storage() {
        return new S3TicketPdfStorage(url, accessKey, secretKey, BUCKET);
    }

    private static String key() {
        return "tickets/" + UUID.randomUUID() + "/" + UUID.randomUUID() + ".pdf";
    }

    private static byte[] read(String key) throws Exception {
        try (InputStream object = admin.getObject(GetObjectArgs.builder().bucket(BUCKET).object(key).build())) {
            return object.readAllBytes();
        }
    }

    private static long versionsOf(String key) throws Exception {
        long versions = 0;
        for (var result : admin.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET).prefix(key).includeVersions(true).build())) {
            Item item = result.get();
            versions += item.objectName().equals(key) ? 1 : 0;
        }
        return versions;
    }

    @Test
    void aNewKeyIsWrittenAndItsReferenceReturned() throws Exception {
        String key = key();
        byte[] pdf = "%PDF-new".getBytes();

        String reference = storage().storeOnce(key, pdf);

        assertEquals("s3://tickets/" + key, reference);
        assertArrayEquals(pdf, read(key));
    }

    @Test
    void aKeyThatAlreadyHasAnObjectIsKeptAndNotWrittenAgain() throws Exception {
        String key = key();
        storage().storeOnce(key, "%PDF-first".getBytes());

        String reference = storage().storeOnce(key, "%PDF-second".getBytes());

        assertEquals("s3://tickets/" + key, reference);
        assertArrayEquals("%PDF-first".getBytes(), read(key));
        assertEquals(1, versionsOf(key));
    }

    @Test
    void aBucketThatDoesNotExistIsAStorageFailure() {
        S3TicketPdfStorage missing = new S3TicketPdfStorage(url, accessKey, secretKey, "no-such-bucket");

        assertThrows(PdfStorageException.class, () -> missing.storeOnce(key(), "%PDF".getBytes()));
    }

    @Test
    void aWrongSecretIsAStorageFailureThatDoesNotLeakTheCredentials() {
        S3TicketPdfStorage wrong = new S3TicketPdfStorage(url, accessKey, "wrong-secret-value", BUCKET);

        PdfStorageException failure = assertThrows(PdfStorageException.class, () -> wrong.storeOnce(key(), "%PDF".getBytes()));
        assertFalse(failure.getMessage().contains("wrong-secret-value"));
        assertFalse(failure.getMessage().contains(accessKey));
    }

    @Test
    void aStorageThatDoesNotAnswerIsAStorageFailure() {
        S3TicketPdfStorage down = new S3TicketPdfStorage("http://127.0.0.1:1", accessKey, secretKey, BUCKET);

        PdfStorageException failure = assertThrows(PdfStorageException.class, () -> down.storeOnce(key(), "%PDF".getBytes()));
        assertTrue(failure.getMessage().startsWith("the object storage"));
    }

    @Test
    void blankCredentialsAreRefusedAtStart() {
        assertThrows(IllegalStateException.class, () -> new S3TicketPdfStorage(url, "", secretKey, BUCKET));
        assertThrows(IllegalStateException.class, () -> new S3TicketPdfStorage(url, accessKey, " ", BUCKET));
    }
}
