package co.edu.corhuila.csp.worker.application.port.out;

/**
 * What the worker needs from a Booking event about a reservation: which event it is and the reservation it
 * speaks of. The rest of the envelope belongs to Booking.
 */
public record ReservationEvent(String eventId, Type type, String reservationId) {

    /** The two Booking events that settle the concession order of a reservation. */
    public enum Type {
        BOOKING_CONFIRMED,
        RESERVATION_EXPIRED
    }
}
