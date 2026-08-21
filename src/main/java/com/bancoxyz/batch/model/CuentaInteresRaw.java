package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Fila cruda del archivo {@code intereses.csv} (cuentas de ahorro y prestamo). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CuentaInteresRaw {

    private Long cuentaId;
    private String nombre;
    private String saldo;
    private String edad;
    private String tipo;
}
