package co.edu.corhuila.csp.worker.app;

import co.edu.corhuila.csp.worker.adapter.out.http.TicketingApiClient;
import co.edu.corhuila.csp.worker.adapter.out.pdf.ITextPdfGeneratorAdapter;
import co.edu.corhuila.csp.worker.adapter.out.storage.MinioObjectStorageAdapter;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcOutboxSource;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcRelayStateStore;
import co.edu.corhuila.csp.worker.adapter.out.outbox.EnvelopeSchemaValidator;
import co.edu.corhuila.csp.worker.adapter.out.outbox.JdbcAdvisoryRunLock;
import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpEventPublisher;
import co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort;
import co.edu.corhuila.csp.worker.application.port.out.storage.ObjectStoragePort;
import co.edu.corhuila.csp.worker.application.port.out.ticketing.TicketingApi;
import co.edu.corhuila.csp.worker.application.port.out.OutboxSource;
import co.edu.corhuila.csp.worker.application.port.out.RelayStateStore;
import co.edu.corhuila.csp.worker.application.port.out.RunLock;
import co.edu.corhuila.csp.worker.application.port.out.EnvelopeValidator;
import co.edu.corhuila.csp.worker.application.port.out.EventPublisher;
import co.edu.corhuila.csp.worker.application.usecase.GenerateTicketPdfJob;
import co.edu.corhuila.csp.worker.application.usecase.RetryPolicy;
import co.edu.corhuila.csp.worker.application.port.in.Job;
import co.edu.corhuila.csp.worker.adapter.in.scheduler.ScheduledJob;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

@Configuration
@ConditionalOnProperty(name = "worker.generate-ticket-pdf.enabled", havingValue = "true", matchIfMissing = false)
public class GenerateTicketPdfConfiguration {

    @Bean
    public OutboxSource bookingOutboxSource(
            @Value("${worker.outbox-relay.database.url}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String user,
            @Value("${worker.outbox-relay.database.password:}") String pass) {
        var ds = new com.zaxxer.hikari.HikariDataSource();
        ds.setJdbcUrl(url); ds.setUsername(user); ds.setPassword(pass); ds.setMaximumPoolSize(2); ds.setPoolName("ticketing-pdf-relay");
        return new JdbcOutboxSource(new org.springframework.jdbc.core.JdbcTemplate(ds), RelayDomain.BOOKING, List.of("BookingConfirmed"));
    }

    @Bean
    public RelayStateStore ticketingRelayStateStore(
            @Value("${worker.outbox-relay.database.url}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String user,
            @Value("${worker.outbox-relay.database.password:}") String pass) {
        var ds = new com.zaxxer.hikari.HikariDataSource();
        ds.setJdbcUrl(url); ds.setUsername(user); ds.setPassword(pass); ds.setMaximumPoolSize(2); ds.setPoolName("ticketing-pdf-relay");
        return new JdbcRelayStateStore(new org.springframework.jdbc.core.JdbcTemplate(ds), RelayDomain.TICKETING);
    }

    @Bean
    public EnvelopeSchemaValidator ticketingEnvelopeValidator() { return new EnvelopeSchemaValidator(RelayDomain.TICKETING); }

    @Bean
    public RunLock ticketingRunLock(
            @Value("${worker.outbox-relay.database.url}") String url,
            @Value("${worker.outbox-relay.database.username:worker_app}") String user,
            @Value("${worker.outbox-relay.database.password:}") String pass) {
        return new JdbcAdvisoryRunLock(new org.springframework.jdbc.datasource.DriverManagerDataSource(url, user, pass), RelayDomain.TICKETING);
    }

    @Bean
    public EventPublisher ticketingEventPublisher(
            @Value("${worker.generate-ticket-pdf.amqp.url:amqp://guest:guest@localhost:5672/%2F}") String url,
            @Value("${worker.generate-ticket-pdf.amqp.confirm-timeout-seconds:5}") long timeout) {
        var f = new com.rabbitmq.client.ConnectionFactory(); f.setUri(url);
        return new AmqpEventPublisher(f, java.time.Duration.ofSeconds(timeout), RelayDomain.TICKETING);
    }

    @Bean public TicketPdfGeneratorPort pdfGenerator() { return new ITextPdfGeneratorAdapter(); }

    @Bean public ObjectStoragePort objectStorage(
            @Value("${worker.generate-ticket-pdf.object-storage.url}") String url,
            @Value("${worker.generate-ticket-pdf.object-storage.access-key}") String ak,
            @Value("${worker.generate-ticket-pdf.object-storage.secret-key}") String sk,
            @Value("${worker.generate-ticket-pdf.object-storage.bucket:tickets}") String bucket) {
        return new MinioObjectStorageAdapter(url, ak, sk, bucket);
    }

    @Bean public TicketingApi ticketingApi(@Value("${worker.generate-ticket-pdf.ticketing-api.url}") String url) {
        return new TicketingApiClient(url);
    }

    @Bean public ScheduledJob generateTicketPdfJob(
            OutboxSource bookingOutboxSource, RelayStateStore state, EnvelopeSchemaValidator validator,
            EventPublisher publisher, RetryPolicy retryPolicy, java.time.Clock clock, RunLock lock,
            co.edu.corhuila.csp.worker.application.port.out.pdf.TicketPdfGeneratorPort pdfGen,
            ObjectStoragePort storage, TicketingApi api,
            @Value("${worker.generate-ticket-pdf.batch-size:10}") int batch,
            @Value("${worker.generate-ticket-pdf.run-timeout-seconds:60}") long timeout) {
        return new ScheduledJob(
            new GenerateTicketPdfJob("generate-ticket-pdf", bookingOutboxSource, state, validator, publisher,
                    new RetryPolicy(Math::random), java.time.Clock.systemUTC(), lock, pdfGen, objectStorage, ticketingApi),
            java.time.Duration.ofSeconds(30));
    }

    static void require(String v, String n) { if (v == null || v.isBlank()) throw new IllegalStateException(n + " required"); }
}