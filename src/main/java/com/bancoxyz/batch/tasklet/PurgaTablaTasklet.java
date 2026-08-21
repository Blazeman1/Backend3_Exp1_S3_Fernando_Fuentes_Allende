package com.bancoxyz.batch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Tasklet generico que trunca (TRUNCATE) una tabla destino antes de cargar los datos del
 * archivo CSV correspondiente. Se usa como primer Step de los Jobs de "Transacciones Diarias"
 * y "Estados de Cuenta Anuales", cuyas tablas no tienen una clave de negocio natural que
 * permita un upsert idempotente (a diferencia de "Calculo de Intereses", que si la tiene:
 * {@code cuenta_id}). De esta forma cada ejecucion del Job es reproducible: no se acumulan
 * filas de corridas anteriores en la evidencia de ejecucion.
 */
public class PurgaTablaTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(PurgaTablaTasklet.class);

    private final JdbcTemplate jdbcTemplate;
    private final String tabla;

    public PurgaTablaTasklet(JdbcTemplate jdbcTemplate, String tabla) {
        this.jdbcTemplate = jdbcTemplate;
        this.tabla = tabla;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("Purgando datos previos de la tabla '{}' antes de cargar la nueva corrida...", tabla);
        jdbcTemplate.execute("TRUNCATE TABLE " + tabla);
        log.info("Tabla '{}' purgada correctamente.", tabla);
        return RepeatStatus.FINISHED;
    }
}
