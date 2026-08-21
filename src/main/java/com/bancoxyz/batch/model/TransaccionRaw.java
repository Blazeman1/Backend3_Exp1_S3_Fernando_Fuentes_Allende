package com.bancoxyz.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Representa una fila cruda (sin validar) del archivo {@code transacciones.csv}.
 *
 * <p>Todos los campos se leen como {@link String} a proposito -aun {@code monto}, que
 * conceptualmente es numerico- porque el dataset legacy puede traer valores vacios,
 * mal formados o simplemente ausentes. Delegar el parseo/validacion al
 * {@link com.bancoxyz.batch.processor.TransaccionItemProcessor} evita que
 * {@code FlatFileItemReader} falle prematuramente con un {@code FlatFileParseException}
 * generico y nos permite aplicar reglas de negocio propias (corregir o descartar).</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransaccionRaw {

    private Long id;
    private String fecha;
    private String monto;
    private String tipo;
}
