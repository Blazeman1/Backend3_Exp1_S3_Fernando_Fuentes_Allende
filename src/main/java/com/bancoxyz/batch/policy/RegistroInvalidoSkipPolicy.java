package com.bancoxyz.batch.policy;

import com.bancoxyz.batch.exception.InvalidDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * Politica de omision (SkipPolicy) personalizada para los tres Jobs del Banco XYZ.
 *
 * <p>Reglas de negocio implementadas (a diferencia de un simple {@code .skip(Class)} +
 * {@code .skipLimit(n)}, aqui se decide explicitamente QUE se omite y se deja constancia
 * en el log de la razon):</p>
 *
 * <ul>
 *   <li>{@link InvalidDataException}: error de validacion de negocio detectado por un
 *       {@code ItemProcessor} (fecha/monto/edad/tipo imposibles de corregir). Se omite
 *       mientras no se supere {@code limiteOmisiones}.</li>
 *   <li>{@link FlatFileParseException}: fila del CSV mal formada a nivel estructural
 *       (columnas de mas/menos, encoding, etc). Se omite igual que el caso anterior.</li>
 *   <li>{@link DataIntegrityViolationException}: violacion de una restriccion UNIQUE en la
 *       base de datos (detecta registros duplicados que el {@code ItemProcessor} no pudo
 *       identificar por si solo, p.ej. dos filas con la misma fecha/monto/tipo). Gracias a
 *       {@code faultTolerant()}, Spring Batch reprocesa el chunk fallido item por item, por
 *       lo que este policy puede aislar exactamente el registro duplicado y omitir solo ese.</li>
 *   <li>Cualquier otra excepcion (por ejemplo, una caida de conexion a la base de datos) NO
 *       se omite: se deja que falle el Step para que el {@code RetryPolicy} intente
 *       recuperarse o, si no puede, el Job termine y quede disponible para re-ejecucion.</li>
 * </ul>
 */
public class RegistroInvalidoSkipPolicy implements SkipPolicy {

    private static final Logger log = LoggerFactory.getLogger(RegistroInvalidoSkipPolicy.class);

    private final String nombreStep;
    private final long limiteOmisiones;

    public RegistroInvalidoSkipPolicy(String nombreStep, long limiteOmisiones) {
        this.nombreStep = nombreStep;
        this.limiteOmisiones = limiteOmisiones;
    }

    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        boolean esOmitible = t instanceof InvalidDataException
                || t instanceof FlatFileParseException
                || t instanceof DataIntegrityViolationException;

        if (!esOmitible) {
            log.error("[{}] Excepcion NO omitible detectada ({}): {}. El step fallara.",
                    nombreStep, t.getClass().getSimpleName(), t.getMessage());
            return false;
        }

        if (skipCount >= limiteOmisiones) {
            log.error("[{}] Se alcanzo el limite de omisiones permitidas ({}). " +
                            "El Step se marcara como fallido para evitar perdida silenciosa de datos.",
                    nombreStep, limiteOmisiones);
            throw new SkipLimitExceededException((int) limiteOmisiones, t);
        }

        log.warn("[{}] Registro omitido ({}/{} permitidas) por {}: {}",
                nombreStep, skipCount + 1, limiteOmisiones, t.getClass().getSimpleName(), t.getMessage());
        return true;
    }
}
