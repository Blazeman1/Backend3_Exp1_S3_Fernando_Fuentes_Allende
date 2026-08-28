package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.exception.InvalidDataException;
import com.bancoxyz.batch.model.MovimientoAnualProcesado;
import com.bancoxyz.batch.model.MovimientoAnualRaw;
import com.bancoxyz.batch.util.FechaFlexibleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.text.Normalizer;
import java.time.LocalDate;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Valida y corrige cada movimiento de {@code cuentas_anuales.csv} antes de dejarlo en la
 * tabla de staging que luego agrega {@link com.bancoxyz.batch.tasklet.AgregacionEstadosCuentaTasklet}.
 *
 * <p>Reglas aplicadas:</p>
 * <ul>
 *   <li><b>Fecha</b>: se corrige cualquiera de los formatos legacy soportados por
 *       {@link FechaFlexibleParser} (verificados contra el dataset real de la semana 3).</li>
 *   <li><b>Tipo de movimiento</b>: se acepta {@code deposito}/{@code retiro}/{@code compra}
 *       (igual que antes) mas {@code pago} -tratado igual que {@code retiro}/{@code compra}:
 *       dinero que sale de la cuenta-, y se <b>normaliza el acento</b> antes de comparar
 *       (se corrige, no se descarta) porque el dataset oficial de la semana 3 trae la variante
 *       bien escrita en espanol {@code depósito} (con tilde) para el 5.2% de las filas: es la
 *       ortografia correcta del propio dominio de negocio, no un dato corrupto, y compararla
 *       sin normalizar habria descartado esas filas por error.</li>
 *   <li><b>Descripcion</b>: si viene vacia o en blanco, se corrige asignando un valor por
 *       defecto en vez de descartar el movimiento (no afecta los totales monetarios del
 *       informe de auditoria).</li>
 *   <li><b>Consistencia de signo</b>: un {@code deposito} debe tener monto positivo y un
 *       {@code retiro}/{@code compra}/{@code pago} debe tener monto negativo (dinero que sale
 *       de la cuenta). Si el signo es inconsistente con el tipo de movimiento se corrige
 *       automaticamente (se invierte el signo) y se deja la anomalia registrada para
 *       auditoria, ya que el requerimiento pide "un informe detallado para auditorias".</li>
 *   <li><b>Monto en cero</b> o <b>tipo de movimiento no reconocido</b>: se descarta (skip),
 *       pues no se puede clasificar de forma confiable dentro del resumen anual.</li>
 * </ul>
 */
public class MovimientoAnualItemProcessor implements ItemProcessor<MovimientoAnualRaw, MovimientoAnualProcesado> {

    private static final Logger log = LoggerFactory.getLogger(MovimientoAnualItemProcessor.class);
    private static final Set<String> TIPOS_VALIDOS = Set.of("deposito", "retiro", "compra", "pago");
    private static final String DESCRIPCION_POR_DEFECTO = "Movimiento sin descripcion registrada";
    private static final Pattern DIACRITICOS = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");

    /**
     * Quita tildes/diacriticos (NFD + eliminar marcas de combinacion) para poder comparar
     * "depósito" contra "deposito" sin depender de que el dataset de origen use siempre la
     * misma ortografia. No se aplica a la descripcion libre ni a otros campos: es
     * deliberadamente puntual, solo para clasificar el tipo de movimiento.
     */
    private static String sinAcentos(String valor) {
        String normalizado = Normalizer.normalize(valor, Normalizer.Form.NFD);
        return DIACRITICOS.matcher(normalizado).replaceAll("");
    }

    @Override
    public MovimientoAnualProcesado process(MovimientoAnualRaw raw) {
        LocalDate fecha = FechaFlexibleParser.parsear(raw.getFecha());
        if (FechaFlexibleParser.esFormatoLegacy(raw.getFecha())) {
            log.info("Cuenta {}: fecha corregida de formato legacy '{}' a '{}'", raw.getCuentaId(), raw.getFecha(), fecha);
        }

        String tipoCrudo = raw.getTransaccion() == null ? "" : raw.getTransaccion().trim().toLowerCase();
        String tipoMovimiento = sinAcentos(tipoCrudo);
        if (!tipoMovimiento.equals(tipoCrudo)) {
            log.info("Cuenta {}: tipo de movimiento corregido de '{}' a '{}' (se normalizo el acento)",
                    raw.getCuentaId(), raw.getTransaccion(), tipoMovimiento);
        }
        if (!TIPOS_VALIDOS.contains(tipoMovimiento)) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() +
                    ": tipo de movimiento no reconocido ('" + raw.getTransaccion() + "')");
        }

        BigDecimal monto = parsearMonto(raw);
        if (monto.compareTo(BigDecimal.ZERO) == 0) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": movimiento con monto en cero, no se puede clasificar");
        }

        String motivoAnomalia = null;
        boolean esDeposito = tipoMovimiento.equals("deposito");
        boolean signoInconsistente = esDeposito
                ? monto.compareTo(BigDecimal.ZERO) < 0
                : monto.compareTo(BigDecimal.ZERO) > 0;

        if (signoInconsistente) {
            motivoAnomalia = "signo de monto corregido automaticamente (no correspondia al tipo de movimiento '" + tipoMovimiento + "')";
            monto = monto.negate();
            log.warn("Cuenta {}: {}", raw.getCuentaId(), motivoAnomalia);
        }

        String descripcion = raw.getDescripcion() == null ? "" : raw.getDescripcion().trim();
        if (descripcion.isBlank()) {
            descripcion = DESCRIPCION_POR_DEFECTO;
            log.info("Cuenta {}: descripcion vacia corregida con valor por defecto", raw.getCuentaId());
        }

        boolean esAnomalia = motivoAnomalia != null;
        return new MovimientoAnualProcesado(raw.getCuentaId(), fecha, tipoMovimiento, monto, descripcion, esAnomalia, motivoAnomalia);
    }

    private BigDecimal parsearMonto(MovimientoAnualRaw raw) {
        if (raw.getMonto() == null || raw.getMonto().isBlank()) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": monto vacio o nulo");
        }
        try {
            return new BigDecimal(raw.getMonto().trim());
        } catch (NumberFormatException e) {
            throw new InvalidDataException("Cuenta " + raw.getCuentaId() + ": monto no numerico ('" + raw.getMonto() + "')", e);
        }
    }
}
