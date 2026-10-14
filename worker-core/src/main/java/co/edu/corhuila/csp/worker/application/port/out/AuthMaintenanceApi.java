package co.edu.corhuila.csp.worker.application.port.out;

/**
 * The identity service as seen by the worker: only the maintenance operation the worker needs. The
 * worker never writes the auth schema.
 */
public interface AuthMaintenanceApi {

    /**
     * Calls {@code POST /internal/maintenance/purge} on the identity service.
     *
     * @param correlationId the correlation id of this purge run
     * @param batchSize how many rows the service may handle in this call, from 1 to 500
     * @return how many rows were handled and whether more remain
     */
    PurgeResult purge(String correlationId, int batchSize);
}
