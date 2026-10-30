package co.edu.corhuila.csp.worker.adapter.out.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.pdf.ITextPdfGeneratorAdapter;
import co.edu.corhuila.csp.worker.adapter.out.ticketing.TicketingPdfClient;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.usecase.RenderTicketPdfsJob;
import com.sun.net.httpserver.HttpServer;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The whole job with the real adapters: a stand-in for csp-ticketing-api lists one ticket and receives the report, iText
 * renders the document and a real S3-compatible storage keeps it under the key of ADR-058. Runs when {@code TEST_S3_URL}
 * is set, like the writer tests.
 */
@EnabledIfEnvironmentVariable(named = "TEST_S3_URL", matches = ".+")
class RenderTicketPdfsEndToEndTest {

    private static final String BUCKET = "tickets";

    @Test
    void aTicketAwaitingItsPdfEndsAsAStoredPdfAndAReportedReference() throws Exception {
        String url = System.getenv("TEST_S3_URL");
        String accessKey = System.getenv("TEST_S3_ACCESS_KEY");
        String secretKey = System.getenv("TEST_S3_SECRET_KEY");
        MinioClient admin = MinioClient.builder().endpoint(url).credentials(accessKey, secretKey).build();
        if (!admin.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
            admin.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }
        UUID ticketId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        List<String> reports = new CopyOnWriteArrayList<>();
        List<String> awaiting = new CopyOnWriteArrayList<>();
        awaiting.add("{\"data\":[{\"ticketId\":\"" + ticketId + "\",\"reservationId\":\"" + reservationId + "\","
                + "\"movieTitle\":\"Dune\",\"roomName\":\"Room 1\",\"showtimeStartsAt\":null,\"seatNumbers\":[\"A1\",\"A2\"],"
                + "\"concessions\":[{\"name\":\"Popcorn\",\"quantity\":1,\"unitPriceCents\":12000}],\"qrPayload\":\"QR\"}]}");
        HttpServer ticketing = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ticketing.createContext("/", exchange -> {
            boolean list = exchange.getRequestMethod().equals("GET");
            if (!list) {
                reports.add(exchange.getRequestURI() + " " + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            }
            // The service lists a ticket until its PDF is reported.
            byte[] body = (list ? (reports.isEmpty() ? awaiting.get(0) : "{\"data\":[]}") : "").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        ticketing.start();
        try {
            RenderTicketPdfsJob job = new RenderTicketPdfsJob(
                    new TicketingPdfClient("http://127.0.0.1:" + ticketing.getAddress().getPort(),
                            new ServiceTokenSource("service-token", ""), 5),
                    new ITextPdfGeneratorAdapter(), new S3TicketPdfStorage(url, accessKey, secretKey, BUCKET), 10,
                    Duration.ofSeconds(30), Clock.systemUTC());

            JobResult first = job.run();
            JobResult second = job.run();

            String key = "tickets/" + reservationId + "/" + ticketId + ".pdf";
            assertEquals(new JobResult(1, 0), first);
            assertEquals(new JobResult(0, 0), second);
            assertEquals(List.of("/internal/tickets/" + ticketId + "/pdf {\"pdfUrl\":\"s3://tickets/" + key + "\"}"), reports);
            try (InputStream object = admin.getObject(GetObjectArgs.builder().bucket(BUCKET).object(key).build())) {
                byte[] stored = object.readAllBytes();
                assertEquals("%PDF", new String(stored, 0, 4, StandardCharsets.US_ASCII));
                assertTrue(stored.length < 1024 * 1024);
            }
        } finally {
            ticketing.stop(0);
        }
    }
}
