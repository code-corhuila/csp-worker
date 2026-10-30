package co.edu.corhuila.csp.worker.adapter.out.ticketing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The client against a stand-in for csp-ticketing-api that answers as its internal routes do: they sit at the root of the
 * service, the list takes the limit as a query parameter and answers {@code {"data": [...]}}, and the report takes the
 * reference of the stored PDF as {@code {"pdfUrl": ...}}.
 */
class TicketingPdfClientTest {

    private static final String TICKET_ID = "11111111-1111-4111-8111-111111111111";
    private static final String RESERVATION_ID = "22222222-2222-4222-8222-222222222222";

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> correlations = new CopyOnWriteArrayList<>();
    private final List<String> contentTypes = new CopyOnWriteArrayList<>();
    private final List<Integer> statuses = new CopyOnWriteArrayList<>();
    private volatile String listAnswer = "{\"data\":[]}";

    @BeforeEach
    void startTheService() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            correlations.add(exchange.getRequestHeaders().getFirst("X-Correlation-Id"));
            contentTypes.add(exchange.getRequestHeaders().getFirst("Content-Type"));
            int status = statuses.isEmpty() ? 200 : statuses.remove(0);
            byte[] body = (status != 200 ? "{\"error\":\"X\"}" : exchange.getRequestMethod().equals("GET") ? listAnswer : "")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopTheService() {
        server.stop(0);
    }

    private TicketingPdfClient client() {
        return new TicketingPdfClient("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                new ServiceTokenSource("service-token", ""), 5);
    }

    @Test
    void awaitingPdfCallsItsRouteWithTheLimitTheTokenAndTheCorrelationIdAndMapsTheTickets() {
        listAnswer = "{\"data\":[{\"ticketId\":\"" + TICKET_ID + "\",\"reservationId\":\"" + RESERVATION_ID + "\","
                + "\"movieTitle\":\"Dune\",\"roomName\":\"Room 1\",\"showtimeStartsAt\":\"2026-11-01T20:00:00Z\","
                + "\"seatNumbers\":[\"A1\",\"A2\"],\"concessions\":[{\"name\":\"Popcorn\",\"quantity\":2,\"unitPriceCents\":12000}],"
                + "\"qrPayload\":\"QR\"}]}";

        List<TicketPdfData> tickets = client().awaitingPdf("run-1", 25);

        assertEquals(List.of("GET /internal/tickets/awaiting-pdf?limit=25"), requests);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-1"), correlations);
        assertEquals(1, tickets.size());
        TicketPdfData ticket = tickets.get(0);
        assertEquals(UUID.fromString(TICKET_ID), ticket.ticketId());
        assertEquals(UUID.fromString(RESERVATION_ID), ticket.reservationId());
        assertEquals("Dune", ticket.movieTitle());
        assertEquals("Room 1", ticket.roomName());
        assertEquals(Instant.parse("2026-11-01T20:00:00Z"), ticket.showtimeStartsAt());
        assertEquals(List.of("A1", "A2"), ticket.seatNumbers());
        assertEquals(List.of(new TicketPdfData.Concession("Popcorn", 2, 12000)), ticket.concessions());
        assertEquals("QR", ticket.qrPayload());
    }

    @Test
    void aTicketWithoutStartTimeNorConcessionsIsMappedWithNullAndAnEmptyList() {
        listAnswer = "{\"data\":[{\"ticketId\":\"" + TICKET_ID + "\",\"reservationId\":\"" + RESERVATION_ID + "\","
                + "\"movieTitle\":\"Dune\",\"roomName\":\"Room 1\",\"showtimeStartsAt\":null,"
                + "\"seatNumbers\":[\"A1\"],\"concessions\":[],\"qrPayload\":\"QR\"}]}";

        TicketPdfData ticket = client().awaitingPdf("run-2", 10).get(0);

        assertNull(ticket.showtimeStartsAt());
        assertTrue(ticket.concessions().isEmpty());
    }

    @Test
    void noTicketAwaitingIsAnEmptyList() {
        assertTrue(client().awaitingPdf("run-3", 10).isEmpty());
    }

    @Test
    void anAnswerThatIsNotTheContractIsAnError() {
        listAnswer = "not json";

        assertThrows(TicketingApiException.class, () -> client().awaitingPdf("run-4", 10));
        listAnswer = "{\"data\":[{\"ticketId\":\"not-a-uuid\"}]}";
        assertThrows(TicketingApiException.class, () -> client().awaitingPdf("run-4", 10));
    }

    @Test
    void registerPdfPostsTheReferenceAsJsonWithTheTokenAndTheCorrelationId() {
        client().registerPdf("run-5", UUID.fromString(TICKET_ID), "s3://tickets/tickets/r/t.pdf");

        assertEquals(List.of("POST /internal/tickets/" + TICKET_ID + "/pdf"), requests);
        assertEquals(List.of("{\"pdfUrl\":\"s3://tickets/tickets/r/t.pdf\"}"), bodies);
        assertEquals(List.of("application/json"), contentTypes);
        assertEquals(List.of("Bearer service-token"), authorizations);
        assertEquals(List.of("run-5"), correlations);
    }

    @Test
    void aRefusalOfTheServiceIsAnError() {
        statuses.add(404);
        assertThrows(TicketingApiException.class,
                () -> client().registerPdf("run-6", UUID.fromString(TICKET_ID), "s3://tickets/x"));
        statuses.add(400);
        assertThrows(TicketingApiException.class, () -> client().awaitingPdf("run-6", 10));
    }

    @Test
    void aNotAnsweringServiceIsAnError() {
        TicketingPdfClient client = new TicketingPdfClient("http://127.0.0.1:1",
                new ServiceTokenSource("service-token", ""), 1);

        assertThrows(TicketingApiException.class, () -> client.awaitingPdf("run-7", 10));
        assertThrows(TicketingApiException.class,
                () -> client.registerPdf("run-7", UUID.fromString(TICKET_ID), "s3://tickets/x"));
    }
}
