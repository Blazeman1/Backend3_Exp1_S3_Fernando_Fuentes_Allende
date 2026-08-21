package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Salida del {@link com.bancoxyz.batch.processor.TransaccionItemProcessor}, lista para
 * ser escrita en la tabla {@code transacciones_procesadas}. Los registros marcados con
 * {@code esAnomalia = true} SI se persisten (para trazabilidad y auditoria), pero quedan
 * etiquetados con el motivo detectado; solo los registros irrecuperables (fecha o monto
 * imposibles de interpretar) se descartan antes de llegar aqui mediante
 * {@link com.bancoxyz.batch.exception.InvalidDataException}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransaccionProcesada {

    private Long idOrigen;
    private LocalDate fecha;
    private BigDecimal monto;
    private String tipo;
    private Boolean esAnomalia;
    private String motivoAnomalia;
}
