package com.bancoxyz.batch.util;

import com.bancoxyz.batch.exception.InvalidDataException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Utilidad compartida para normalizar las fechas de los archivos legacy del Banco XYZ.
 *
 * <p>El repositorio de datos legacy (README de
 * <a href="https://github.com/KariVillagran/bank_legacy_data">bank_legacy_data</a>) advierte
 * explicitamente que las fechas llegan en dos formatos posibles: {@code yyyy-MM-dd} (formato
 * correcto) y {@code yyyy/MM/dd} (formato legacy incorrecto). Esta clase intenta ambos formatos
 * y, si ninguno aplica, lanza {@link InvalidDataException} para que la {@code SkipPolicy}
 * personalizada decida omitir el registro.</p>
 */
public final class FechaFlexibleParser {

    private static final DateTimeFormatter FORMATO_ESTANDAR = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATO_LEGACY = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private FechaFlexibleParser() {
        // utilidad estatica, no instanciable
    }

    /**
     * Intenta parsear la fecha probando primero el formato estandar ISO ({@code yyyy-MM-dd})
     * y luego el formato legacy con slash ({@code yyyy/MM/dd}).
     *
     * @param valorCrudo valor tal como llega del CSV
     * @return la fecha normalizada
     * @throws InvalidDataException si el valor es nulo/blanco o no calza con ningun formato soportado
     */
    public static LocalDate parsear(String valorCrudo) {
        if (valorCrudo == null || valorCrudo.isBlank()) {
            throw new InvalidDataException("Fecha vacia o nula");
        }
        String valor = valorCrudo.trim();
        try {
            return LocalDate.parse(valor, FORMATO_ESTANDAR);
        } catch (DateTimeParseException primeraFallida) {
            try {
                return LocalDate.parse(valor, FORMATO_LEGACY);
            } catch (DateTimeParseException segundaFallida) {
                throw new InvalidDataException("Formato de fecha no reconocido: '" + valorCrudo + "'");
            }
        }
    }

    /**
     * Igual que {@link #parsear(String)} pero indicando si el valor original ya estaba en el
     * formato estandar, util para que los procesadores puedan registrar cuantas fechas debieron
     * corregirse (metrica de calidad de datos).
     */
    public static boolean esFormatoLegacy(String valorCrudo) {
        String valor = valorCrudo.trim();
        try {
            LocalDate.parse(valor, FORMATO_ESTANDAR);
            return false;
        } catch (DateTimeParseException e) {
            return true;
        }
    }
}
