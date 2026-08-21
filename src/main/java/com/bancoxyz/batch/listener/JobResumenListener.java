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
