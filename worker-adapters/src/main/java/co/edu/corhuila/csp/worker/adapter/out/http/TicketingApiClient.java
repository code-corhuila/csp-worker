package co.edu.corhuila.csp.worker.adapter.out.http;

import co.edu.corhuila.csp.worker.application.port.out.ticketing.TicketingApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Component
public class TicketingApiClient implements TicketingApi {
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper om = new ObjectMapper();
    private final String baseUrl;

    public TicketingApiClient(@Value("${worker.generate-ticket-pdf.ticketing-api.url}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public void submitPdfUrl(String ticketId, String pdfUrl) {
        try {
            var body = new ObjectMapper().writeValueAsString(new PdfUrlRequest(pdfUrl));
            var req = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/internal/tickets/" + ticketId + "/pdf"))
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
            var resp = java.net.http.HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) throw new RuntimeException("API error: " + resp.statusCode() + " " + resp.body());
        } catch (Exception e) { throw new RuntimeException("API call failed", e); }
    }

    record PdfUrlRequest(String pdfUrl) {}
}