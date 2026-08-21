package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Salida del {@link com.bancoxyz.batch.processor.InteresItemProcessor}. Contiene el saldo
 * final ya con el interes mensual aplicado, lista para persistirse (upsert) en la tabla
 * {@code cuentas_interes}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CuentaInteresProcesada {

    private Long cuentaId;
    private String nombre;
    private String tipo;
    private Integer edad;
    private BigDecimal saldoInicial;
    private BigDecimal tasaInteresMensual;
    private BigDecimal interesCalculado;
    private BigDecimal saldoFinal;
}
