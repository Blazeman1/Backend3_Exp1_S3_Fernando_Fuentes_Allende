package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Fila cruda del archivo {@code cuentas_anuales.csv} (historial de movimientos por cuenta). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MovimientoAnualRaw {

    private Long cuentaId;
    private String fecha;
    private String transaccion;
    private String monto;
    private String descripcion;
}
