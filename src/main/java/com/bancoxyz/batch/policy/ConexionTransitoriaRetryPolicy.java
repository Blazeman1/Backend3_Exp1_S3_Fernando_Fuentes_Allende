package com.bancoxyz.batch.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.List;
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
 *
 * <p><b>Nota de una corrida real (evidencia de GitHub Actions, 22-08-2026):</b> un
 * {@code DuplicateKeyException} (no transitorio, delegado a
 * {@link RegistroInvalidoSkipPolicy}) hizo que {@link #canRetry} devolviera {@code false}
 * de inmediato -correcto-, pero la version anterior de esta clase registraba ese caso con
 * el mismo mensaje de ERROR que usa cuando SI se agotan reintentos reales de una falla
 * transitoria ("Se agotaron los N reintentos permitidos"), lo cual sugiere erroneamente que
 * el sistema lo intento 3 veces y desistio. En realidad nunca fue elegible para reintento.
 * Por eso {@link #esFallaTransitoria} clasifica la excepcion de forma independiente (sin
 * depender del metodo privado {@code retryForException} de la superclase) para distinguir
 * ambos casos y loguear cada uno con el nivel y el mensaje que le corresponde.</p>
 */
public class ConexionTransitoriaRetryPolicy extends SimpleRetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(ConexionTransitoriaRetryPolicy.class);

    private static final List<Class<? extends Throwable>> EXCEPCIONES_TRANSITORIAS = List.of(
            TransientDataAccessException.class,
            SQLTransientException.class,
            QueryTimeoutException.class
    );

    private final String nombreStep;

    public ConexionTransitoriaRetryPolicy(String nombreStep, int maximoIntentos) {
        super(maximoIntentos, excepcionesTransitoriasReintentables(), true);
        this.nombreStep = nombreStep;
    }

    private static Map<Class<? extends Throwable>, Boolean> excepcionesTransitoriasReintentables() {
        Map<Class<? extends Throwable>, Boolean> reintentables = new HashMap<>();
        for (Class<? extends Throwable> tipo : EXCEPCIONES_TRANSITORIAS) {
            reintentables.put(tipo, true);
        }
        return reintentables;
    }

    /**
     * Reclasifica la excepcion de forma independiente a la superclase (cuyo
     * {@code retryForException} es privado) para poder distinguir, en el logging, entre
     * "nunca fue candidata a reintento" y "era candidata pero se agotaron los intentos".
     * Recorre la cadena de causas porque el constructor de {@link SimpleRetryPolicy} se
     * invoca con {@code traverseCauses = true}.
     */
    private boolean esFallaTransitoria(Throwable t) {
        if (t == null) {
            return false;
        }
        for (Class<? extends Throwable> tipo : EXCEPCIONES_TRANSITORIAS) {
            if (tipo.isInstance(t)) {
                return true;
            }
        }
        return esFallaTransitoria(t.getCause());
    }

    @Override
    public boolean canRetry(RetryContext context) {
        boolean puedeReintentar = super.canRetry(context);
        Throwable ultimaExcepcion = context.getLastThrowable();
        if (ultimaExcepcion == null) {
            return puedeReintentar;
        }

        if (puedeReintentar) {
            log.warn("[{}] Intento {}/{} tras falla transitoria ({}): {}",
                    nombreStep, context.getRetryCount(), getMaxAttempts(),
                    ultimaExcepcion.getClass().getSimpleName(), ultimaExcepcion.getMessage());
        } else if (esFallaTransitoria(ultimaExcepcion)) {
            log.error("[{}] Se agotaron los {} reintentos permitidos ante una falla transitoria real. Ultima causa: {}",
                    nombreStep, getMaxAttempts(), ultimaExcepcion.getMessage());
        } else {
            log.debug("[{}] {} no es una falla transitoria conocida; no se reintenta y se delega a la politica de omision: {}",
                    nombreStep, ultimaExcepcion.getClass().getSimpleName(), ultimaExcepcion.getMessage());
        }
        return puedeReintentar;
    }
}
