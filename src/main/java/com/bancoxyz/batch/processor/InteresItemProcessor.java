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
import java.util.concurrent.ConcurrentHashMap;

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
 *   <li>Registro duplicado: mismo titular, saldo, edad y tipo ya visto en esta corrida
 *       (independientemente del numero de cuenta), replicando el problema de "registros
 *       duplicados" descrito en el dataset legacy.</li>
 * </ul>
 *
 * <p><b>Nota de diseno (thread-safety):</b> este processor se declara como bean singleton
 * (no {@code @StepScope}) para poder mantener, en {@link #firmasVistas}, un registro de las
 * combinaciones nombre+saldo+edad+tipo ya procesadas y detectar asi duplicados entre los 3
 * hilos del Step multithread. Se usa {@link ConcurrentHashMap#newKeySet()} porque es seguro
 * para escritura concurrente. Como contrapartida, el estado vive mientras viva el contexto de
 * Spring: en este proyecto no es un problema porque cada ejecucion de Job corresponde a un
 * proceso Java independiente (ver {@code BancoXyzBatchApplication}), pero en una aplicacion de
 * larga duracion esta deduplicacion deberia resolverse contra la base de datos.</p>
 */
public class InteresItemProcessor implements ItemProcessor<CuentaInteresRaw, CuentaInteresProcesada> {

    private static final Logger log = LoggerFactory.getLogger(InteresItemProcessor.class);

    private static final Set<String> TIPOS_SOPORTADOS = Set.of("ahorro", "prestamo");
    private static final int EDAD_MINIMA = 18;
    private static final int EDAD_MAXIMA = 90;
    private static final BigDecimal TASA_AHORRO = new BigDecimal("0.0150");
    private static final BigDecimal TASA_PRESTAMO = new BigDecimal("0.0250");

    private final Set<String> firmasVistas = ConcurrentHashMap.newKeySet();

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
        String firma = (nombre.toLowerCase() + "|" + saldo + "|" + edad + "|" + tipo);
        if (!firmasVistas.add(firma)) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() +
                    ": registro duplicado (mismo titular/saldo/edad/tipo ya procesado en esta corrida)");
        }

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
