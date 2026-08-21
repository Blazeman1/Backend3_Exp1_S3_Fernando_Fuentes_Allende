package com.bancoxyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

/**
 * {@code SkipListener} generico (parametrizado con {@code <Object, Object>} para poder
 * reutilizarse en los tres Jobs) que deja constancia detallada de cada registro omitido,
 * distinguiendo si el fallo ocurrio durante la lectura, el procesamiento (reglas de negocio)
 * o la escritura (por ejemplo, una violacion de restriccion UNIQUE por duplicados).
 *
 * <p>Junto con {@link com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy}, este listener
 * cumple el criterio de la pauta "Maneja los errores y excepciones usando politicas y
 * listeners, garantizando la continuidad del proceso en caso de fallos": el Step no se
 * detiene, pero cada omision queda trazada en el log para su posterior auditoria.</p>
 */
public class RegistroOmitidoListener implements SkipListener<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(RegistroOmitidoListener.class);

    private final String nombreStep;

    public RegistroOmitidoListener(String nombreStep) {
        this.nombreStep = nombreStep;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("[{}] OMISION en LECTURA - {}: {}", nombreStep, t.getClass().getSimpleName(), t.getMessage());
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        log.warn("[{}] OMISION en PROCESAMIENTO - item='{}' - {}: {}",
                nombreStep, item, t.getClass().getSimpleName(), t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        log.warn("[{}] OMISION en ESCRITURA (probable duplicado u otra restriccion de BD) - item='{}' - {}: {}",
                nombreStep, item, t.getClass().getSimpleName(), t.getMessage());
    }
}
