package com.bancoxyz.batch.exception;

/**
 * Excepcion de negocio lanzada por los distintos {@code ItemProcessor} del proyecto
 * cuando un registro proveniente de los CSV legacy del Banco XYZ no puede ser
 * corregido automaticamente y, por lo tanto, no es seguro escribirlo en la base
 * de datos relacional.
 *
 * <p>Esta excepcion es intencionalmente NO verificada (extiende {@link RuntimeException})
 * para poder ser lanzada libremente dentro de la firma de {@code ItemProcessor#process}.
 * Es capturada exclusivamente por {@link com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy},
 * que decide si el registro debe omitirse (skip) respetando un limite maximo de omisiones
 * por Step.</p>
 */
public class InvalidDataException extends RuntimeException {

    public InvalidDataException(String mensaje) {
        super(mensaje);
    }

    public InvalidDataException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
