package com.bancoxyz.batch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Segundo Step del Job "Generacion de Estados de Cuenta Anuales". El Step 1 (chunk-oriented,
 * paralelo) valida y carga cada movimiento individual en la tabla de staging
 * {@code cuentas_anuales_movimientos}; este Tasklet compila -mediante una unica sentencia SQL
 * de agregacion- el informe detallado por cuenta que exigen las instrucciones especificas
 * ("Compilar datos anuales para cada cuenta y generar un informe detallado para auditorias"),
 * dejandolo en la tabla {@code estados_cuenta_anuales}.
 *
 * <p>Se usa {@code INSERT ... ON CONFLICT (cuenta_id) DO UPDATE} para que el reporte sea
 * idempotente ante una re-ejecucion del Job dentro del mismo anio.</p>
 */
public class AgregacionEstadosCuentaTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(AgregacionEstadosCuentaTasklet.class);

    private static final String SQL_AGREGACION = """
            INSERT INTO estados_cuenta_anuales
                (cuenta_id, anio, total_depositos, total_retiros, total_compras, saldo_neto, cantidad_movimientos, cantidad_anomalias)
            SELECT
                cuenta_id,
                EXTRACT(YEAR FROM fecha)::INT AS anio,
                COALESCE(SUM(monto) FILTER (WHERE tipo_movimiento = 'deposito'), 0) AS total_depositos,
                COALESCE(SUM(monto) FILTER (WHERE tipo_movimiento = 'retiro'), 0) AS total_retiros,
                COALESCE(SUM(monto) FILTER (WHERE tipo_movimiento = 'compra'), 0) AS total_compras,
                COALESCE(SUM(monto), 0) AS saldo_neto,
                COUNT(*) AS cantidad_movimientos,
                COUNT(*) FILTER (WHERE es_anomalia = TRUE) AS cantidad_anomalias
            FROM cuentas_anuales_movimientos
            GROUP BY cuenta_id, EXTRACT(YEAR FROM fecha)
            ON CONFLICT (cuenta_id) DO UPDATE SET
                anio = EXCLUDED.anio,
                total_depositos = EXCLUDED.total_depositos,
                total_retiros = EXCLUDED.total_retiros,
                total_compras = EXCLUDED.total_compras,
                saldo_neto = EXCLUDED.saldo_neto,
                cantidad_movimientos = EXCLUDED.cantidad_movimientos,
                cantidad_anomalias = EXCLUDED.cantidad_anomalias,
                fecha_generacion = now()
            """;

    private final JdbcTemplate jdbcTemplate;

    public AgregacionEstadosCuentaTasklet(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        int cuentasAgregadas = jdbcTemplate.update(SQL_AGREGACION);
        Integer totalCuentas = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM estados_cuenta_anuales", Integer.class);
        log.info("Estados de cuenta anuales generados/actualizados para {} cuenta(s). Total historico en tabla: {}.",
                cuentasAgregadas, totalCuentas);
        return RepeatStatus.FINISHED;
    }
}
