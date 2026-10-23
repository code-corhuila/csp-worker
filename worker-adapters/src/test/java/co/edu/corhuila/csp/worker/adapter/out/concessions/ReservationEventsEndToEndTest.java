package co.edu.corhuila.csp.worker.adapter.out.concessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import co.edu.corhuila.csp.worker.adapter.out.amqp.AmqpReservationEvents;
import co.edu.corhuila.csp.worker.adapter.out.booking.ServiceTokenSource;
import co.edu.corhuila.csp.worker.application.port.in.JobResult;
import co.edu.corhuila.csp.worker.application.usecase.ReservationEventsJob;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * HU-CONCESSIONS-004, scenario 3, from the broker to the service: a Booking event published to the events
 * exchange reaches the queue of Concessions, the job calls the stand-in of the concessions service with the
 * worker's token and the message is settled as the answer says. Runs when {@code TEST_AMQP_URL} is set.
 */
@EnabledIfEnvironmentVariable(named = "TEST_AMQP_URL", matches = ".+")
class ReservationEventsEndToEndTest {

    private static final String EXCHANGE = "cine.events.e2e";
    private static final String DEAD_LETTER_EXCHANGE = "cine.dlx.e2e";
    private static final String CONFIRMED_QUEUE = "cine.concessions.booking-confirmed";
    private static final String EXPIRED_QUEUE = "cine.concessions.reservation-expired";
    private static final String CONTEXT = "/api/v1/concessions";
    private static final String RESERVATION = "44444444-4444-4444-8444-444444444444";

    private HttpServer server;
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final List<String> tokens = new CopyOnWriteArrayList<>();
    private volatile int answer = 204;
    private Connection connection;
    private Channel channel;
    private AmqpReservationEvents queues;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            tokens.add(exchange.getRequestHeaders().getFirst("Authorization"));
            exchange.getRequestBody().readAllBytes();
            if (answer == 204) {
                exchange.sendResponseHeaders(204, -1);
            } else {
                byte[] body = "{\"error\":\"X\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(answer, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(URI.create(System.getenv("TEST_AMQP_URL")));
        connection = factory.newConnection("reservation-events-e2e");
        channel = connection.createChannel();
        channel.confirmSelect();
        channel.exchangeDeclare(EXCHANGE, "topic", true);
        channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "topic", true);
        queues = new AmqpReservationEvents(factory, EXCHANGE, DEAD_LETTER_EXCHANGE);
        queues.next();
    }

    @AfterEach
    void stop() throws Exception {
        server.stop(0);
        queues.close();
        for (String queue : new String[] {CONFIRMED_QUEUE, EXPIRED_QUEUE}) {
            channel.queueDelete(queue);
            channel.queueDelete(queue + ".dlq");
        }
        channel.exchangeDelete(EXCHANGE);
        channel.exchangeDelete(DEAD_LETTER_EXCHANGE);
        connection.close();
    }

    private void publish(String type, String routingKey) throws Exception {
        String body = "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"" + type + "\",\"version\":1,"
                + "\"payload\":{\"reservationId\":\"" + RESERVATION + "\"}}";
        channel.basicPublish(EXCHANGE, routingKey, true,
                new AMQP.BasicProperties.Builder().contentType("application/json").deliveryMode(2).build(),
                body.getBytes(StandardCharsets.UTF_8));
        channel.waitForConfirmsOrDie(2_000);
    }

    private JobResult run() {
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + CONTEXT;
        ConcessionsOrdersClient client = new ConcessionsOrdersClient(url, new ServiceTokenSource("worker-token", ""), 5);
        return new ReservationEventsJob(queues, client, 10, Duration.ofSeconds(20), Clock.systemUTC()).run();
    }

    private int depth(String queue) throws Exception {
        return channel.queueDeclarePassive(queue).getMessageCount();
    }

    @Test
    void theEventsOfTheReservationConfirmAndReleaseItsOrderAndLeaveTheQueues() throws Exception {
        publish("BookingConfirmed", "cine.booking.booking-confirmed.v1");
        publish("ReservationExpired", "cine.booking.reservation-expired.v1");

        JobResult result = run();

        assertEquals(new JobResult(2, 0), result);
        assertEquals(2, calls.size());
        assertEquals(true, calls.contains("POST " + CONTEXT + "/internal/orders/by-reservation/" + RESERVATION + "/confirm"));
        assertEquals(true, calls.contains("POST " + CONTEXT + "/internal/orders/by-reservation/" + RESERVATION + "/release"));
        assertEquals(List.of("Bearer worker-token", "Bearer worker-token"), tokens);
        Thread.sleep(100);
        assertEquals(0, depth(CONFIRMED_QUEUE));
        assertEquals(0, depth(EXPIRED_QUEUE));
    }

    @Test
    void aMessageTheServiceRefusesForGoodEndsInTheDeadLetterQueue() throws Exception {
        answer = 409;
        publish("BookingConfirmed", "cine.booking.booking-confirmed.v1");

        JobResult result = run();

        assertEquals(new JobResult(0, 1), result);
        Thread.sleep(200);
        assertEquals(0, depth(CONFIRMED_QUEUE));
        assertEquals(1, depth(CONFIRMED_QUEUE + ".dlq"));
    }

    @Test
    void whenTheServiceFailsTheMessageStaysInTheQueueAndIsSettledByALaterRun() throws Exception {
        answer = 503;
        publish("BookingConfirmed", "cine.booking.booking-confirmed.v1");

        JobResult failed = run();

        assertEquals(new JobResult(0, 1), failed);
        Thread.sleep(200);
        assertEquals(1, depth(CONFIRMED_QUEUE));
        assertNotNull(tokens.get(0));

        answer = 204;
        assertEquals(new JobResult(1, 0), run());
        Thread.sleep(100);
        assertEquals(0, depth(CONFIRMED_QUEUE));
        assertEquals(0, depth(CONFIRMED_QUEUE + ".dlq"));
    }
}
