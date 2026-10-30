package co.edu.corhuila.csp.worker.adapter.out.ticketing;

import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.adapter.out.http.RetryingHttp;
import co.edu.corhuila.csp.worker.application.port.out.TicketingApiException;
import co.edu.corhuila.csp.worker.application.port.out.TicketingPdfApi;
import co.edu.corhuila.csp.worker.application.usecase.TicketPdfData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * HTTP client for the internal operations of the Ticketing service about the document of a ticket (ADR-057): the list
 * of the tickets without a PDF and the report of a stored one. The worker authenticates with its service token
 * (Norma 5.7.4) and sends the correlation id of the run on every call (Norma 5.7.3). The internal routes of Ticketing
 * sit at the root of the service, not under {@code /api/v1/ticketing}. It retries only what a retry can fix, as
 * {@link RetryingHttp} does.
 */
@Component
@ConditionalOnProperty(prefix = "worker.render-ticket-pdfs", name = "enabled", havingValue = "true")
public class TicketingPdfClient implements TicketingPdfApi {

    private static final String AWAITING_ROUTE = "/internal/tickets/awaiting-pdf";

    private final RetryingHttp http;
    private final ObjectMapper json = new ObjectMapper();
    private final String baseUrl;
    private final ServiceTokenSource serviceToken;
    private final Duration timeout;

    /** @param ticketingApiUrl the base of the Ticketing service, where its internal routes start */
    public TicketingPdfClient(
            @Value("${ticketing.api.url:http://ticketing-api:8084}") String ticketingApiUrl,
            ServiceTokenSource serviceToken,
            @Value("${ticketing.api.timeout-seconds:10}") long timeoutSeconds) {
        this.baseUrl = ticketingApiUrl.replaceAll("/+$", "");
        this.serviceToken = serviceToken;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.http = new RetryingHttp(HttpClient.newBuilder().connectTimeout(timeout).build(), "ticketing service");
    }

    @Override
    public List<TicketPdfData> awaitingPdf(String correlationId, int limit) {
        String answer = send(request(correlationId, AWAITING_ROUTE + "?limit=" + limit).GET().build());
        try {
            List<TicketPdfData> tickets = new ArrayList<>();
            for (JsonNode node : json.readTree(answer).path("data")) {
                tickets.add(ticketOf(node));
            }
            return tickets;
        } catch (IOException | RuntimeException exception) {
            throw new TicketingApiException("the answer of the ticketing service is not the list of tickets", exception);
        }
    }

    @Override
    public void registerPdf(String correlationId, UUID ticketId, String pdfUrl) {
        String body = json.createObjectNode().put("pdfUrl", pdfUrl).toString();
        send(request(correlationId, "/internal/tickets/" + ticketId + "/pdf")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build());
    }

    private HttpRequest.Builder request(String correlationId, String route) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + route))
                .header("Authorization", "Bearer " + serviceToken.current())
                .header("X-Correlation-Id", correlationId)
                .timeout(timeout);
    }

    private String send(HttpRequest request) {
        try {
            return http.send(request);
        } catch (RetryingHttp.Failure failure) {
            throw new TicketingApiException(failure.getMessage(), failure);
        }
    }

    private static TicketPdfData ticketOf(JsonNode node) {
        List<String> seats = new ArrayList<>();
        node.path("seatNumbers").forEach(seat -> seats.add(seat.asText()));
        List<TicketPdfData.Concession> concessions = new ArrayList<>();
        node.path("concessions").forEach(concession -> concessions.add(new TicketPdfData.Concession(
                concession.path("name").asText(), concession.path("quantity").asInt(),
                concession.path("unitPriceCents").asLong())));
        JsonNode startsAt = node.path("showtimeStartsAt");
        return new TicketPdfData(UUID.fromString(node.path("ticketId").asText()),
                UUID.fromString(node.path("reservationId").asText()), node.path("movieTitle").asText(),
                node.path("roomName").asText(), startsAt.isNull() || startsAt.isMissingNode() ? null : Instant.parse(startsAt.asText()),
                seats, concessions, node.path("qrPayload").asText());
    }
}
