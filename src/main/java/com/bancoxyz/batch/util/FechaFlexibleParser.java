package com.bancoxyz.batch.util;

import com.bancoxyz.batch.exception.InvalidDataException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Utilidad compartida para normalizar las fechas de los archivos legacy del Banco XYZ.
 *
 * <p>El README general de
 * <a href="https://github.com/KariVillagran/bank_legacy_data">bank_legacy_data</a> advierte,
 * en terminos generales, que las fechas llegan en dos formatos posibles: {@code yyyy-MM-dd}
 * (formato correcto) y {@code yyyy/MM/dd} (formato legacy incorrecto). Sin embargo, el dataset
 * OFICIAL de la semana 3 (carpeta {@code data/semana_3} del repositorio, usado por esta
 * actividad sumativa) trae en la practica <b>cuatro</b> formatos distintos, verificado
 * programaticamente sobre el propio CSV: {@code yyyy-MM-dd} (294 filas), {@code yyyy/MM/dd}
 * (222), y ademas {@code dd-MM-yyyy} (250) y {@code dd/MM/yyyy} (234) - dia primero, convencion
 * habitual en Chile/Latinoamerica, confirmada sin ambiguedad porque una parte de esas fechas
 * trae un primer campo mayor a 12 (imposible como mes). Esta clase intenta los cuatro formatos
 * en orden y, si ninguno aplica (por ejemplo, {@code 2024-13-01}: estructura valida pero mes
 * 13 inexistente, tambien presente en el dataset real), lanza {@link InvalidDataException} para
 * que la {@code SkipPolicy} personalizada decida omitir el registro.</p>
 */
public final class FechaFlexibleParser {

    private static final DateTimeFormatter FORMATO_ESTANDAR = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATO_LEGACY_SLASH_AMD = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    private static final DateTimeFormatter FORMATO_LEGACY_GUION_DMA = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final DateTimeFormatter FORMATO_LEGACY_SLASH_DMA = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private FechaFlexibleParser() {
        // utilidad estatica, no instanciable
    }

    /**
     * Intenta parsear la fecha probando, en orden, el formato estandar ISO
     * ({@code yyyy-MM-dd}) y los tres formatos legacy observados en el dataset real de la
     * semana 3: {@code yyyy/MM/dd}, {@code dd-MM-yyyy} y {@code dd/MM/yyyy}.
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
        for (DateTimeFormatter formato : new DateTimeFormatter[] {
                FORMATO_ESTANDAR, FORMATO_LEGACY_SLASH_AMD, FORMATO_LEGACY_GUION_DMA, FORMATO_LEGACY_SLASH_DMA
        }) {
            try {
                return LocalDate.parse(valor, formato);
            } catch (DateTimeParseException ignorada) {
                // se intenta el siguiente formato soportado
            }
        }
        throw new InvalidDataException("Formato de fecha no reconocido: '" + valorCrudo + "'");
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
