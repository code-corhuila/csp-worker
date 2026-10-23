package co.edu.corhuila.csp.worker.adapter.out.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import co.edu.corhuila.csp.worker.adapter.out.outbox.RelayDomain;
import org.junit.jupiter.api.Test;

/**
 * Contract of the routing keys of Concessions in its {@code events.md}: the key drops the word "Concession"
 * of the event type, so {@code ConcessionOrderConfirmed} is {@code order-confirmed} and not
 * {@code concession-order-confirmed}.
 */
class ConcessionsRoutingKeysTest {

    @Test
    void everyConcessionsEventIsRoutedWithTheKeyOfEventsMd() {
        assertEquals("cine.concessions.order-created.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.CONCESSIONS, "ConcessionOrderCreated", 1));
        assertEquals("cine.concessions.stock-reserved.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.CONCESSIONS, "ConcessionStockReserved", 1));
        assertEquals("cine.concessions.order-confirmed.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.CONCESSIONS, "ConcessionOrderConfirmed", 1));
        assertEquals("cine.concessions.order-cancelled.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.CONCESSIONS, "ConcessionOrderCancelled", 1));
    }

    @Test
    void theOtherDomainsKeepTheirKeys() {
        assertEquals("cine.booking.booking-confirmed.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.BOOKING, "BookingConfirmed", 1));
        assertEquals("cine.auth.user-registered.v1",
                AmqpEventPublisher.routingKeyOf(RelayDomain.AUTH, "UserRegistered", 1));
    }
}
