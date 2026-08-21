package com.bancoxyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

/**
 * {@code JobExecutionDecider} que implementa la politica de finalizacion mencionada en la
 * guia de aprendizaje ("Control de finalizacion (JobExecutionDecider)"). Tras ejecutarse el
 * Step de carga/validacion de cada Job, este decider calcula la proporcion de registros
 * omitidos (skip ratio) sobre el total leido y decide el siguiente tramo del flujo:
 *
 * <ul>
 *   <li>Si la proporcion de omisiones supera {@code umbralPorcentaje}, el flujo deriva hacia
 *       un Step de "revision requerida" (se deja constancia en el log y el Job termina con un
 *       exit status especial {@code REVISION_REQUERIDA}, sin generar el reporte/resumen final,
 *       ya que la calidad de los datos de origen no es confiable).</li>
 *   <li>En caso contrario, el flujo continua normalmente hacia el Step de agregacion/resumen.</li>
 * </ul>
 *
 * <p>Esto ademas deja el Job en un estado claro para decidir si corresponde una
 * re-ejecucion (por ejemplo, tras corregir el archivo de origen) usando el mismo
 * {@code JobRepository}, que persiste el estado de la ejecucion.</p>
 */
public class ControlCalidadDecider implements JobExecutionDecider {

    private static final Logger log = LoggerFactory.getLogger(ControlCalidadDecider.class);

    public static final String CALIDAD_OK = "CALIDAD_OK";
    public static final String REVISION_REQUERIDA = "REVISION_REQUERIDA";

    private final double umbralPorcentaje;

    public ControlCalidadDecider(double umbralPorcentaje) {
        this.umbralPorcentaje = umbralPorcentaje;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (stepExecution == null) {
            return new FlowExecutionStatus(CALIDAD_OK);
        }

        long leidos = stepExecution.getReadCount();
        long omitidos = stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount();
        long totalIntentado = leidos + stepExecution.getReadSkipCount();
        double porcentajeOmision = totalIntentado > 0 ? (omitidos * 100.0 / totalIntentado) : 0.0;

        log.info("[ControlCalidadDecider] Step [{}]: {} omitidos de {} intentados ({}%). Umbral configurado: {}%",
                stepExecution.getStepName(), omitidos, totalIntentado, String.format("%.1f", porcentajeOmision), umbralPorcentaje);

        if (porcentajeOmision > umbralPorcentaje) {
            log.warn("[ControlCalidadDecider] Porcentaje de omision ({}%) supera el umbral ({}%). " +
                            "Se deriva el flujo a revision manual antes de generar el reporte final.",
                    String.format("%.1f", porcentajeOmision), umbralPorcentaje);
            return new FlowExecutionStatus(REVISION_REQUERIDA);
        }

        return new FlowExecutionStatus(CALIDAD_OK);
    }
}
