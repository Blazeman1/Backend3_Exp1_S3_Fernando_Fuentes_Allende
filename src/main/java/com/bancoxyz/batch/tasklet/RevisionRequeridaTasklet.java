package com.bancoxyz.batch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Tasklet ejecutado unicamente cuando {@link com.bancoxyz.batch.listener.ControlCalidadDecider}
 * determina que el porcentaje de registros omitidos superó el umbral aceptable. En un entorno
 * productivo aqui se enviaria una alerta (correo, Slack, ticket) al equipo de datos; en este
 * proyecto formativo se deja registrado en el log con nivel ERROR y se detiene el flujo sin
 * generar el reporte/resumen final, evitando publicar informacion poco confiable.
 */
public class RevisionRequeridaTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(RevisionRequeridaTasklet.class);

    private final String nombreProceso;

    public RevisionRequeridaTasklet(String nombreProceso) {
        this.nombreProceso = nombreProceso;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.error("*** ATENCION: el proceso '{}' NO generara su reporte final. ***", nombreProceso);
        log.error("*** El porcentaje de registros con errores en el archivo de origen supero el umbral permitido. ***");
        log.error("*** Corrija los datos de origen y vuelva a ejecutar el Job (el JobRepository permite re-ejecucion). ***");
        contribution.setExitStatus(new org.springframework.batch.core.ExitStatus("REVISION_REQUERIDA",
                "Calidad de datos insuficiente para " + nombreProceso));
        return RepeatStatus.FINISHED;
    }
}
