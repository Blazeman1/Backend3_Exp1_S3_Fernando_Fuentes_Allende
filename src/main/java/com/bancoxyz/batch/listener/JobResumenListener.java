package com.bancoxyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;

import java.time.Duration;

/**
 * {@code JobExecutionListener} comun a los tres Jobs. Al finalizar (exitosamente o no)
 * imprime un resumen consolidado con la duracion total y las metricas agregadas de todos
 * los Steps, lo cual sirve como evidencia rapida de ejecucion sin tener que revisar el log
 * completo linea por linea.
 */
public class JobResumenListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobResumenListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("############################################################");
        log.info("INICIO JOB [{}] - parametros={}",
                jobExecution.getJobInstance().getJobName(), jobExecution.getJobParameters());
        log.info("############################################################");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Duration duracion = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime());
        long totalLeidos = 0;
        long totalEscritos = 0;
        long totalOmitidos = 0;

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            // IMPORTANTE (evidencia real de ejecucion, benchmark de particionado S3): en un Step
            // particionado, Spring Batch nombra cada ejecucion "hija" como "step:particionN" y
            // ADEMAS agrega (copia la suma) esos mismos contadores en el StepExecution del Step
            // "manager" (aqui, "particionTransaccionesStep"), que tambien aparece en
            // jobExecution.getStepExecutions(). Sumar TODAS las StepExecution sin distincion
            // duplica por dos los totales del Job cuando hay particionado (se confirmo con
            // evidencia real: leidos=2000/escritos=1522/omitidos=478, exactamente el doble de los
            // valores correctos). La convencion de nombres "step:particion" es la unica marca
            // disponible para distinguir una ejecucion "hija" del Step manager que ya la agrega,
            // asi que se omiten aqui para no contar dos veces lo mismo; los Steps normales
            // (no particionados) nunca llevan ":" en su nombre, por lo que este filtro no afecta
            // a ningun otro Job del proyecto.
            if (stepExecution.getStepName().contains(":")) {
                continue;
            }
            totalLeidos += stepExecution.getReadCount();
            totalEscritos += stepExecution.getWriteCount();
            totalOmitidos += stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount();
        }

        log.info("############################################################");
        log.info("FIN JOB [{}] - estado={} | duracion={} ms | leidos={} escritos={} omitidos={}",
                jobExecution.getJobInstance().getJobName(), jobExecution.getStatus(),
                duracion.toMillis(), totalLeidos, totalEscritos, totalOmitidos);
        log.info("Estado de salida (exit status): {}", jobExecution.getExitStatus().getExitCode());
        if (!jobExecution.getExitStatus().getExitDescription().isBlank()) {
            log.info("Descripcion: {}", jobExecution.getExitStatus().getExitDescription());
        }
        log.info("############################################################");
    }
}
