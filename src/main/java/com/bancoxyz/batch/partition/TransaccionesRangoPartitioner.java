package com.bancoxyz.batch.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Particiona {@code transacciones.csv} en {@code gridSize} rangos de filas de tamano similar,
 * usando indices de item (1-based, sin contar el encabezado) como limites {@code start}/{@code end}.
 *
 * <p>Esta es la tecnica de escalamiento alternativa a la del Job 2/3 (multi-hilo sobre un unico
 * Step): en vez de que N hilos compitan por leer del mismo {@code ItemReader} sincronizado, cada
 * particion es una ejecucion de Step INDEPENDIENTE (su propio {@code StepExecution}, su propia
 * instancia de {@code FlatFileItemReader} abriendo el archivo por su cuenta) que procesa una
 * porcion disjunta de los datos. El {@code TaskExecutorPartitionHandler} (ver
 * {@link com.bancoxyz.batch.config.TransaccionesParticionadoJobConfig}) es quien lanza esas
 * ejecuciones en paralelo sobre un {@code TaskExecutor}.</p>
 *
 * <p>Contar las lineas del archivo aqui, en {@link #partition(int)}, es intencional: este metodo
 * corre una sola vez, en el hilo del Step manager, ANTES de lanzar las particiones - es el lugar
 * natural para decidir como se reparte el trabajo. Contraste con el enfoque de S2: alli la
 * paralelizacion no requeria conocer el tamano total de antemano, porque un unico
 * {@code ItemReader} sincronizado iba entregando items bajo demanda a los 3 hilos.</p>
 */
public class TransaccionesRangoPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(TransaccionesRangoPartitioner.class);

    private final Resource recursoCsv;

    public TransaccionesRangoPartitioner(Resource recursoCsv) {
        this.recursoCsv = recursoCsv;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long totalFilas = contarFilasDeDatos();
        long tamanoBase = totalFilas / gridSize;
        long resto = totalFilas % gridSize;

        Map<String, ExecutionContext> particiones = new LinkedHashMap<>();
        long inicio = 1;
        for (int i = 0; i < gridSize && inicio <= totalFilas; i++) {
            // Las primeras "resto" particiones absorben una fila extra para repartir el residuo
            // de la division entera sin dejar filas fuera de rango.
            long tamanoDeEsta = tamanoBase + (i < resto ? 1 : 0);
            if (tamanoDeEsta == 0) {
                break;
            }
            long fin = Math.min(inicio + tamanoDeEsta - 1, totalFilas);

            ExecutionContext contexto = new ExecutionContext();
            contexto.putLong("start", inicio);
            contexto.putLong("end", fin);
            contexto.putString("nombreParticion", "particion" + i);
            particiones.put("particion" + i, contexto);

            log.info("Particion[{}] asignada: filas {} a {} ({} filas)", i, inicio, fin, fin - inicio + 1);
            inicio = fin + 1;
        }

        log.info("Particionado de transacciones.csv: {} filas de datos repartidas en {} particion(es) real(es) (gridSize solicitado={})",
                totalFilas, particiones.size(), gridSize);
        return particiones;
    }

    /**
     * Cuenta las filas de datos del CSV (todas las lineas menos el encabezado). Se abre el
     * recurso una vez, de forma efimera, solo para contar - no se reutiliza esta lectura para
     * el procesamiento real, que lo hace cada {@code FlatFileItemReader} de cada particion.
     */
    private long contarFilasDeDatos() {
        try (BufferedReader lector = new BufferedReader(
                new InputStreamReader(recursoCsv.getInputStream(), StandardCharsets.UTF_8))) {
            long lineas = lector.lines().count();
            return Math.max(lineas - 1, 0); // -1 por el encabezado
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo contar las filas de " + recursoCsv.getDescription(), e);
        }
    }
}
