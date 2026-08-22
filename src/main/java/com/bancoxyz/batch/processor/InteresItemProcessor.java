package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.exception.InvalidDataException;
import com.bancoxyz.batch.model.CuentaInteresProcesada;
import com.bancoxyz.batch.model.CuentaInteresRaw;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;

/**
 * Valida las cuentas de {@code intereses.csv} y calcula el interes mensual sobre cuentas de
 * ahorro y prestamo, segun exigen las instrucciones especificas (punto de "Requerimientos del
 * Sistema": "Aplicar intereses sobre cuentas de ahorro y prestamos, y actualizar el saldo
 * final en una base de datos").
 *
 * <p><b>Reglas de descarte (skip)</b> - en este Job, a diferencia del de transacciones, un
 * registro invalido NO puede corregirse ni conservarse: sin un tipo de cuenta soportado, una
 * edad plausible y un saldo valido no es posible calcular un interes confiable, por lo que se
 * omite y se deja trazabilidad en el log:</p>
 * <ul>
 *   <li>Tipo de cuenta distinto de {@code ahorro} o {@code prestamo} (p.ej. {@code hipoteca},
 *       {@code -1} o vacio: fuera del alcance de este proceso).</li>
 *   <li>Edad vacia, no numerica o fuera del rango plausible 18-90.</li>
 *   <li>Saldo vacio, no numerico o negativo.</li>
 *   <li>Registro duplicado: mismo titular, saldo, edad y tipo ya visto, dado de alta bajo
 *       otro numero de cuenta, replicando el problema de "registros duplicados" descrito en
 *       el dataset legacy. A diferencia de las tres reglas anteriores, esta NO se valida en
 *       este processor sino en la restriccion {@code UNIQUE uq_cuenta_interes_natural} de la
 *       tabla {@code cuentas_interes}, que el writer viola con un
 *       {@code DuplicateKeyException} (ver nota de evidencia real mas abajo).</li>
 * </ul>
 *
 * <p><b>Nota de una corrida real (evidencia de GitHub Actions, 22-08-2026):</b> la primera
 * version de este processor detectaba el duplicado logico con un {@code Set<String>} en
 * memoria (campo de instancia de un bean singleton). Contra el dataset generado (300 filas,
 * ~3% de duplicados reales,
 * verificado programaticamente sobre el propio CSV) esa version reporto 171 omisiones
 * (57%) en vez de las ~85 (28%) que corresponden a las reglas de validacion realmente
 * aplicadas. La causa: cuando un chunk falla (falla cualquiera de sus items) y hay
 * skip/retry configurados, Spring Batch reprocesa el chunk item por item ("scanning") para
 * aislar cual item es el culpable - y eso vuelve a invocar {@code process()} sobre items que
 * ya habian sido procesados (con exito) en el intento fallido anterior. Como el {@code Set}
 * en memoria no participa de la transaccion JDBC, no se revierte cuando el chunk hace
 * rollback: el reintento encuentra su propia firma ya agregada y se auto-reporta como
 * "duplicado" de si mismo. La deteccion de duplicados se movio por eso a una restriccion
 * {@code UNIQUE} en la tabla {@code cuentas_interes} ({@code uq_cuenta_interes_natural} en
 * {@code schema.sql}), el mismo mecanismo -ya validado con evidencia real en Job 1- que si es
 * seguro bajo reintentos: al vivir dentro de la transaccion del chunk, se revierte junto con
 * ella, y un item valido reprocesado en el "scan" no encuentra ningun conflicto espurio.</p>
 */
public class InteresItemProcessor implements ItemProcessor<CuentaInteresRaw, CuentaInteresProcesada> {

    private static final Logger log = LoggerFactory.getLogger(InteresItemProcessor.class);

    private static final Set<String> TIPOS_SOPORTADOS = Set.of("ahorro", "prestamo");
    private static final int EDAD_MINIMA = 18;
    private static final int EDAD_MAXIMA = 90;
    private static final BigDecimal TASA_AHORRO = new BigDecimal("0.0150");
    private static final BigDecimal TASA_PRESTAMO = new BigDecimal("0.0250");

    @Override
    public CuentaInteresProcesada process(CuentaInteresRaw raw) {
        String tipo = raw.getTipo() == null ? "" : raw.getTipo().trim().toLowerCase();
        if (!TIPOS_SOPORTADOS.contains(tipo)) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": tipo de cuenta no soportado ('" + raw.getTipo() + "')");
        }

        int edad = parsearEdad(raw);
        if (edad < EDAD_MINIMA || edad > EDAD_MAXIMA) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": edad fuera de rango permitido [" +
                    EDAD_MINIMA + "-" + EDAD_MAXIMA + "] (valor=" + edad + ")");
        }

        BigDecimal saldo = parsearSaldo(raw);
        if (saldo.compareTo(BigDecimal.ZERO) < 0) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": saldo negativo no permitido (" + saldo + ")");
        }

        String nombre = raw.getNombre() == null ? "" : raw.getNombre().trim();
        // El duplicado logico (mismo titular/saldo/edad/tipo bajo otro cuenta_id) ya NO se
        // detecta aqui: lo hace la restriccion UNIQUE uq_cuenta_interes_natural al escribir
        // (ver nota de evidencia real en el javadoc de esta clase). El writer traduce esa
        // violacion en un DuplicateKeyException, que la RegistroInvalidoSkipPolicy omite
        // igual que cualquier otro registro invalido.

        BigDecimal tasa = tipo.equals("ahorro") ? TASA_AHORRO : TASA_PRESTAMO;
        BigDecimal interes = saldo.multiply(tasa).setScale(2, RoundingMode.HALF_UP);
        BigDecimal saldoFinal = saldo.add(interes).setScale(2, RoundingMode.HALF_UP);

        log.info("Cuenta {} ({}): saldo={} tasa={}% interes={} saldoFinal={} [hilo={}]",
                raw.getCuentaId(), tipo, saldo, tasa.multiply(BigDecimal.valueOf(100)), interes, saldoFinal,
                Thread.currentThread().getName());

        return new CuentaInteresProcesada(raw.getCuentaId(), nombre, tipo, edad, saldo, tasa, interes, saldoFinal);
    }

    private int parsearEdad(CuentaInteresRaw raw) {
        if (raw.getEdad() == null || raw.getEdad().isBlank()) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": edad vacia o nula");
        }
        try {
            return Integer.parseInt(raw.getEdad().trim());
        } catch (NumberFormatException e) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": edad no numerica ('" + raw.getEdad() + "')", e);
        }
    }

    private BigDecimal parsearSaldo(CuentaInteresRaw raw) {
        if (raw.getSaldo() == null || raw.getSaldo().isBlank()) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": saldo vacio o nulo");
        }
        try {
            return new BigDecimal(raw.getSaldo().trim());
        } catch (NumberFormatException e) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": saldo no numerico ('" + raw.getSaldo() + "')", e);
        }
    }
}
