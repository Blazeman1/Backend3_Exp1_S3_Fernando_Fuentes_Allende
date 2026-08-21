package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Salida del {@link com.bancoxyz.batch.processor.MovimientoAnualItemProcessor}. Se persiste
 * en la tabla de staging {@code cuentas_anuales_movimientos}; posteriormente el Step 2 del Job
 * de estados de cuenta anuales agrega estos movimientos por cuenta en {@code estados_cuenta_anuales}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MovimientoAnualProcesado {

    private Long cuentaId;
    private LocalDate fecha;
    private String tipoMovimiento;
    private BigDecimal monto;
    private String descripcion;
    private Boolean esAnomalia;
    private String motivoAnomalia;
}
