package com.bancoxyz.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Punto de entrada de la migracion batch del Banco XYZ (Exp1 - Semana 2).
 *
 * <p>La auto-ejecucion de Jobs de Spring Boot esta deshabilitada
 * ({@code spring.batch.job.enabled=false} en {@code application.yml}) porque este proyecto
 * expone tres Jobs distintos y se necesita elegir cual ejecutar (y con que parametros) desde
 * la linea de comandos, tal como exige el requerimiento de "Configurar un Job para cada uno de
 * los tres procesos".</p>
 *
 * <h3>Uso</h3>
 * <pre>
 * java -jar banco-xyz-batch.jar transacciones
 * java -jar banco-xyz-batch.jar intereses
 * java -jar banco-xyz-batch.jar cuentas-anuales
 * java -jar banco-xyz-batch.jar todos
 *
 * # Semana 3: modo alternativo de escalamiento (particionado real) para el Job de
 * # transacciones diarias. No forma parte de 'todos' a proposito, para no alterar la
 * # evidencia ya validada del modo multi-hilo; se ejecuta explicitamente:
 * java -jar banco-xyz-batch.jar transacciones-particionado
 * java -jar banco-xyz-batch.jar transacciones-particionado --batch.grid-size=6
 *
 * # Para reintentar/re-ejecutar EXACTAMENTE la misma instancia de Job (por ejemplo, tras
 * # corregir el archivo de origen luego de una falla), se puede fijar el runId manualmente:
 * java -jar banco-xyz-batch.jar transacciones --runId=2026-08-21-01
 * </pre>
 *
 * <p><b>Nota de una corrida real (evidencia de GitHub Actions, 22-08-2026):</b> el Job
 * terminaba en estado {@code COMPLETED} (visible en el log) pero el proceso Java nunca
 * devolvia el control al shell, dejando el step de CI "colgado" indefinidamente y sin poder
 * avanzar al Job siguiente. La causa es el {@code ThreadPoolTaskExecutor} de
 * {@code InfraestructuraBatchConfig}: con {@code corePoolSize == maxPoolSize == 3} y sin
 * {@code allowCoreThreadTimeOut}, esos 3 hilos ("Batch-Thread-1/2/3") son NO-daemon y quedan
 * vivos indefinidamente esperando la proxima tarea, incluso despues de que el
 * {@code CommandLineRunner} termina. Como {@code main()} solo retorna de
 * {@link SpringApplication#run} sin forzar el cierre de la JVM, esta nunca decide terminar
 * mientras esos hilos sigan vivos. La solucion (patron oficial de Spring Boot para
 * aplicaciones de linea de comandos) es cerrar el contexto explicitamente con
 * {@link SpringApplication#exit} y forzar la salida con {@link System#exit}: eso dispara el
 * apagado ordenado del executor (ya configurado con
 * {@code waitForTasksToCompleteOnShutdown=true} y 30s de espera) y garantiza que la JVM
 * termine con el codigo de salida correcto en vez de quedar colgada.</p>
 */
@SpringBootApplication
public class BancoXyzBatchApplication {

    private static final Logger log = LoggerFactory.getLogger(BancoXyzBatchApplication.class);

    /**
     * Codigo de salida del proceso, fijado por el {@link CommandLineRunner} segun el
     * resultado real de los Jobs ejecutados. Se lee desde {@code main()} DESPUES de que
     * {@link SpringApplication#run} retorna (los CommandLineRunner ya se ejecutaron para ese
     * punto), por lo que no requiere sincronizacion adicional mas alla de la visibilidad que
     * ya da {@link AtomicInteger}.
     */
    private static final AtomicInteger codigoSalida = new AtomicInteger(0);

    public static void main(String[] args) {
        ConfigurableApplicationContext contexto = SpringApplication.run(BancoXyzBatchApplication.class, args);
        System.exit(SpringApplication.exit(contexto, codigoSalida::get));
    }

    @Bean
    public CommandLineRunner lanzadorDeJobs(JobLauncher jobLauncher,
                                             @Qualifier("transaccionesDiariasJob") Job transaccionesDiariasJob,
                                             @Qualifier("transaccionesDiariasParticionadoJob") Job transaccionesDiariasParticionadoJob,
                                             @Qualifier("interesesMensualesJob") Job interesesMensualesJob,
                                             @Qualifier("estadosCuentaAnualesJob") Job estadosCuentaAnualesJob) {
        return args -> {
            List<String> argumentos = Arrays.asList(args);
            if (argumentos.isEmpty()) {
                log.error("Debe indicar que Job ejecutar: 'transacciones', 'transacciones-particionado', 'intereses', 'cuentas-anuales' o 'todos'.");
                log.error("Ejemplo: java -jar banco-xyz-batch.jar transacciones");
                codigoSalida.set(1);
                return;
            }

            String seleccion = argumentos.get(0).toLowerCase();
            String runId = argumentos.stream()
                    .filter(a -> a.startsWith("--runId="))
                    .map(a -> a.substring("--runId=".length()))
                    .findFirst()
                    .orElse(String.valueOf(System.currentTimeMillis()));

            // 'todos' recorre unicamente los 3 Jobs base (linea de base multi-hilo), en el mismo
            // orden que la evidencia de la Semana 2, para no alterar esa evidencia ya validada.
            // El Job particionado (Semana 3) es un modo ALTERNATIVO y se invoca explicitamente
            // por nombre, junto al resto de las corridas del benchmark comparativo.
            Map<String, Job> jobsBase = new LinkedHashMap<>();
            jobsBase.put("transacciones", transaccionesDiariasJob);
            jobsBase.put("intereses", interesesMensualesJob);
            jobsBase.put("cuentas-anuales", estadosCuentaAnualesJob);

            Map<String, Job> jobsDisponibles = new LinkedHashMap<>(jobsBase);
            jobsDisponibles.put("transacciones-particionado", transaccionesDiariasParticionadoJob);

            if ("todos".equals(seleccion)) {
                for (Map.Entry<String, Job> entry : jobsBase.entrySet()) {
                    if (!ejecutar(jobLauncher, entry.getValue(), runId)) {
                        codigoSalida.set(1);
                    }
                }
                return;
            }

            Job job = jobsDisponibles.get(seleccion);
            if (job == null) {
                log.error("Job desconocido: '{}'. Opciones validas: {}, todos", seleccion, jobsDisponibles.keySet());
                codigoSalida.set(1);
                return;
            }
            if (!ejecutar(jobLauncher, job, runId)) {
                codigoSalida.set(1);
            }
        };
    }

    /**
     * Ejecuta un Job y devuelve {@code true} solo si termino en {@link BatchStatus#COMPLETED}.
     * Antes de esta correccion el runner ignoraba el resultado de {@code jobLauncher.run(...)},
     * asi que un Job que terminara en {@code FAILED} igual dejaba el proceso con exit code 0 -
     * una CI "en verde" para una corrida que en realidad fallo.
     */
    private boolean ejecutar(JobLauncher jobLauncher, Job job, String runId) throws Exception {
        JobParameters parametros = new JobParametersBuilder()
                .addString("fechaProceso", LocalDate.now().toString())
                .addString("runId", runId)
                .toJobParameters();
        JobExecution ejecucion = jobLauncher.run(job, parametros);
        boolean exitoso = ejecucion.getStatus() == BatchStatus.COMPLETED;
        if (!exitoso) {
            log.error("El Job [{}] termino con estado {} (exit status: {})",
                    job.getName(), ejecucion.getStatus(), ejecucion.getExitStatus().getExitCode());
        }
        return exitoso;
    }
}
