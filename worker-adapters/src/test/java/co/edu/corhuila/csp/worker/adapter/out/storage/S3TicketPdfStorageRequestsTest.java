package co.edu.corhuila.csp.worker.adapter.out.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What the writer sends to the storage. The writer account of ADR-058 may only put and get {@code tickets/*}: a request for
 * anything else, such as the location of the bucket that a client asks for when it does not know its region, is refused with
 * {@code AccessDenied} and the whole write fails. The stand-in answers like an empty bucket and records the requests.
 */
class S3TicketPdfStorageRequestsTest {

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startTheStorage() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            exchange.getRequestBody().readAllBytes();
            switch (exchange.getRequestMethod()) {
                case "HEAD" -> exchange.sendResponseHeaders(404, -1);
                case "PUT" -> {
                    exchange.getResponseHeaders().add("ETag", "\"etag\"");
                    exchange.sendResponseHeaders(200, -1);
                }
                default -> exchange.sendResponseHeaders(403, -1);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopTheStorage() {
        server.stop(0);
    }

    @Test
    void aWriteAsksOnlyForTheKeyItWritesNeverForTheLocationOfTheBucket() {
        S3TicketPdfStorage storage = new S3TicketPdfStorage("http://127.0.0.1:" + server.getAddress().getPort(),
                "ticketing_pdf_writer", "secret", "tickets", "us-east-1");

        storage.storeOnce("tickets/r/t.pdf", "%PDF".getBytes());

        assertEquals(List.of("HEAD /tickets/tickets/r/t.pdf", "PUT /tickets/tickets/r/t.pdf"), requests);
    }
}
