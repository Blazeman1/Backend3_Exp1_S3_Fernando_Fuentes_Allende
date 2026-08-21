package com.bancoxyz.batch.processor;

import com.bancoxyz.batch.exception.InvalidDataException;
import com.bancoxyz.batch.model.TransaccionProcesada;
import com.bancoxyz.batch.model.TransaccionRaw;
import com.bancoxyz.batch.util.FechaFlexibleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Valida, corrige y clasifica cada transaccion diaria leida de {@code transacciones.csv}.
 *
 * <p>Filosofia aplicada (ver instrucciones especificas, punto 2: "Aplica transformaciones y
 * validaciones con un ItemProcessor para corregir o manejar los errores en los datos"):</p>
 * <ol>
 *   <li><b>Se corrige</b> lo que es tecnicamente recuperable: formato de fecha legacy
 *       ({@code yyyy/MM/dd} -> {@code yyyy-MM-dd}).</li>
 *   <li><b>Se marca como anomalia pero se conserva</b> lo que es una senal de negocio valida
 *       aunque sospechosa (montos negativos, en cero, o un tipo de transaccion no reconocido):
 *       el Job 1 existe justamente para "detectar anomalias y generar un resumen", por lo que
 *       descartar esos registros seria contrario al objetivo del proceso.</li>
 *   <li><b>Se descarta (skip)</b> unicamente lo irrecuperable: fecha o monto imposibles de
 *       interpretar. Los duplicados exactos (misma fecha/monto/tipo) se detectan mas abajo en
 *       la capa de escritura, mediante una restriccion UNIQUE en base de datos.</li>
 * </ol>
 */
public class TransaccionItemProcessor implements ItemProcessor<TransaccionRaw, TransaccionProcesada> {

    private static final Logger log = LoggerFactory.getLogger(TransaccionItemProcessor.class);
    private static final Set<String> TIPOS_VALIDOS = Set.of("DEBITO", "CREDITO");

    @Override
    public TransaccionProcesada process(TransaccionRaw raw) {
        List<String> motivosAnomalia = new ArrayList<>();

        LocalDate fecha = FechaFlexibleParser.parsear(raw.getFecha());
        if (FechaFlexibleParser.esFormatoLegacy(raw.getFecha())) {
            log.info("Transaccion id={}: fecha corregida de formato legacy '{}' a '{}'",
                    raw.getId(), raw.getFecha(), fecha);
        }

        BigDecimal monto = parsearMonto(raw);

        String tipo = raw.getTipo() == null ? "" : raw.getTipo().trim().toUpperCase();
        if (!TIPOS_VALIDOS.contains(tipo)) {
            motivosAnomalia.add("tipo de transaccion no reconocido ('" + raw.getTipo() + "')");
        }

        if (monto.compareTo(BigDecimal.ZERO) < 0) {
            motivosAnomalia.add("monto negativo");
        } else if (monto.compareTo(BigDecimal.ZERO) == 0) {
            motivosAnomalia.add("monto en cero");
        }

        boolean esAnomalia = !motivosAnomalia.isEmpty();
        String motivo = esAnomalia ? String.join("; ", motivosAnomalia) : null;

        if (esAnomalia) {
            log.warn("Transaccion id={} marcada como ANOMALIA: {}", raw.getId(), motivo);
        }

        return new TransaccionProcesada(raw.getId(), fecha, monto, tipo.isBlank() ? "DESCONOCIDO" : tipo, esAnomalia, motivo);
    }

    private BigDecimal parsearMonto(TransaccionRaw raw) {
        if (raw.getMonto() == null || raw.getMonto().isBlank()) {
            throw new InvalidDataException("Transaccion id=" + raw.getId() + ": monto vacio o nulo, no se puede procesar");
        }
        try {
            return new BigDecimal(raw.getMonto().trim());
        } catch (NumberFormatException e) {
            throw new InvalidDataException("Transaccion id=" + raw.getId() + ": monto no numerico ('" + raw.getMonto() + "')", e);
        }
    }
}
