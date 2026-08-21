package com.bancoxyz.batch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;

/**
 * Segundo Step del Job "Reporte de Transacciones Diarias". Una vez que el Step 1 valido,
 * corrigio y cargo cada transaccion en {@code transacciones_procesadas} (marcando anomalias
 * pero sin descartarlas), este Tasklet compila el resumen ejecutivo requerido por las
 * instrucciones especificas ("generar un resumen") en la tabla {@code resumen_transacciones_diarias}.
 */
public class ResumenTransaccionesDiariasTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(ResumenTransaccionesDiariasTasklet.class);

    private final JdbcTemplate jdbcTemplate;

    public ResumenTransaccionesDiariasTasklet(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        Integer totalProcesadas = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transacciones_procesadas", Integer.class);
        Integer totalAnomalias = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transacciones_procesadas WHERE es_anomalia = TRUE", Integer.class);
        BigDecimal totalCreditos = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(monto), 0) FROM transacciones_procesadas WHERE tipo = 'CREDITO'", BigDecimal.class);
        BigDecimal totalDebitos = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(monto), 0) FROM transacciones_procesadas WHERE tipo = 'DEBITO'", BigDecimal.class);

        long totalOmitidas = contribution.getStepExecution().getJobExecution().getStepExecutions().stream()
                .mapToLong(se -> se.getReadSkipCount() + se.getProcessSkipCount() + se.getWriteSkipCount())
                .sum();

        jdbcTemplate.update(
                "INSERT INTO resumen_transacciones_diarias " +
                        "(total_procesadas, total_anomalias, total_omitidas, monto_total_creditos, monto_total_debitos) " +
                        "VALUES (?, ?, ?, ?, ?)",
                totalProcesadas, totalAnomalias, totalOmitidas, totalCreditos, totalDebitos);

        log.info("Resumen diario generado -> procesadas={}, anomalias={}, omitidas={}, total creditos={}, total debitos={}",
                totalProcesadas, totalAnomalias, totalOmitidas, totalCreditos, totalDebitos);

        return RepeatStatus.FINISHED;
    }
}
