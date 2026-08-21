package com.bancoxyz.batch.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.Map;

/**
 * Politica de reintento (RetryPolicy) personalizada para los Steps batch del Banco XYZ.
 *
 * <p>Extiende {@link SimpleRetryPolicy} en lugar de usar el registro por defecto
 * ({@code .retry(Exception.class)}) para dejar explicito que SOLO se reintentan fallas
 * transitorias de infraestructura -tipicamente problemas de red o de disponibilidad
 * momentanea de la base de datos-, nunca errores de datos (esos los maneja la
 * {@link RegistroInvalidoSkipPolicy}). Ademas agrega logging propio en cada intento, lo que
 * permite -durante la evidencia de ejecucion- ver claramente cuando el sistema esta
 * reintentando una operacion y cuando decide desistir.</p>
 */
public class ConexionTransitoriaRetryPolicy extends SimpleRetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(ConexionTransitoriaRetryPolicy.class);

    private final String nombreStep;

    public ConexionTransitoriaRetryPolicy(String nombreStep, int maximoIntentos) {
        super(maximoIntentos, excepcionesTransitoriasReintentables(), true);
        this.nombreStep = nombreStep;
    }

    private static Map<Class<? extends Throwable>, Boolean> excepcionesTransitoriasReintentables() {
        Map<Class<? extends Throwable>, Boolean> reintentables = new HashMap<>();
        reintentables.put(TransientDataAccessException.class, true);
        reintentables.put(SQLTransientException.class, true);
        reintentables.put(QueryTimeoutException.class, true);
        return reintentables;
    }

    @Override
    public boolean canRetry(RetryContext context) {
        boolean puedeReintentar = super.canRetry(context);
        Throwable ultimaExcepcion = context.getLastThrowable();
        if (ultimaExcepcion != null) {
            if (puedeReintentar) {
                log.warn("[{}] Intento {}/{} tras falla transitoria ({}): {}",
                        nombreStep, context.getRetryCount(), getMaxAttempts(),
                        ultimaExcepcion.getClass().getSimpleName(), ultimaExcepcion.getMessage());
            } else {
                log.error("[{}] Se agotaron los {} reintentos permitidos. Ultima causa: {}",
                        nombreStep, getMaxAttempts(), ultimaExcepcion.getMessage());
            }
        }
        return puedeReintentar;
    }
}
