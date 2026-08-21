package com.bancoxyz.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Listener de rendimiento aplicado a todos los Steps de tipo chunk.
 *
 * <p>Registra en el log, con nivel INFO, metricas clave de cada Step al finalizar:
 * cantidad leida, procesada, escrita, omitida (skip), cantidad de commits (chunks)
 * y el tiempo total junto al throughput (items/segundo). Estos logs son la evidencia
 * que permite evaluar el rendimiento del proceso batch y decidir si el tamano de chunk
 * o el numero de hilos configurados deben ajustarse (criterio de la pauta: "Implementa
 * tecnicas de logs para evaluar el rendimiento y ajustar configuraciones").</p>
 */
public class RendimientoStepListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(RendimientoStepListener.class);

    private LocalDateTime inicio;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        inicio = LocalDateTime.now();
        log.info("==> Inicio Step [{}] de Job [{}] - hilo principal: {}",
                stepExecution.getStepName(), stepExecution.getJobExecution().getJobInstance().getJobName(),
                Thread.currentThread().getName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LocalDateTime fin = LocalDateTime.now();
        long milisegundos = Duration.between(inicio, fin).toMillis();
        long leidos = stepExecution.getReadCount();
        long escritos = stepExecution.getWriteCount();
        long omitidosLectura = stepExecution.getReadSkipCount();
        long omitidosProceso = stepExecution.getProcessSkipCount();
        long omitidosEscritura = stepExecution.getWriteSkipCount();
        long totalOmitidos = omitidosLectura + omitidosProceso + omitidosEscritura;
        long commits = stepExecution.getCommitCount();
        double itemsPorSegundo = milisegundos > 0 ? (escritos * 1000.0 / milisegundos) : escritos;

        log.info("<== Fin Step [{}] estado={} | leidos={} escritos={} omitidos(total={}, lectura={}, proceso={}, escritura={}) | " +
                        "chunks(commits)={} | duracion={} ms | throughput={} items/seg",
                stepExecution.getStepName(), stepExecution.getExitStatus().getExitCode(),
                leidos, escritos, totalOmitidos, omitidosLectura, omitidosProceso, omitidosEscritura,
                commits, milisegundos, String.format("%.2f", itemsPorSegundo));

        if (totalOmitidos > 0) {
            double porcentajeOmision = leidos > 0 ? (totalOmitidos * 100.0 / (leidos + omitidosLectura)) : 0;
            log.warn("[{}] {} registro(s) omitido(s) ({}% del total procesado). Revisar log de WARN de RegistroInvalidoSkipPolicy / RegistroOmitidoListener para detalle.",
                    stepExecution.getStepName(), totalOmitidos, String.format("%.1f", porcentajeOmision));
        }

        return stepExecution.getExitStatus();
    }
}
