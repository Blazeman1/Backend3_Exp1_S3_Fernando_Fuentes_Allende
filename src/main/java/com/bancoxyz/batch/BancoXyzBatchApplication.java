package com.bancoxyz.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
 * # Para reintentar/re-ejecutar EXACTAMENTE la misma instancia de Job (por ejemplo, tras
 * # corregir el archivo de origen luego de una falla), se puede fijar el runId manualmente:
 * java -jar banco-xyz-batch.jar transacciones --runId=2026-08-21-01
 * </pre>
 */
@SpringBootApplication
public class BancoXyzBatchApplication {

    private static final Logger log = LoggerFactory.getLogger(BancoXyzBatchApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(BancoXyzBatchApplication.class, args);
    }

    @Bean
    public CommandLineRunner lanzadorDeJobs(JobLauncher jobLauncher,
                                             @Qualifier("transaccionesDiariasJob") Job transaccionesDiariasJob,
                                             @Qualifier("interesesMensualesJob") Job interesesMensualesJob,
                                             @Qualifier("estadosCuentaAnualesJob") Job estadosCuentaAnualesJob) {
        return args -> {
            List<String> argumentos = Arrays.asList(args);
            if (argumentos.isEmpty()) {
                log.error("Debe indicar que Job ejecutar: 'transacciones', 'intereses', 'cuentas-anuales' o 'todos'.");
                log.error("Ejemplo: java -jar banco-xyz-batch.jar transacciones");
                System.exit(1);
                return;
            }

            String seleccion = argumentos.get(0).toLowerCase();
            String runId = argumentos.stream()
                    .filter(a -> a.startsWith("--runId="))
                    .map(a -> a.substring("--runId=".length()))
                    .findFirst()
                    .orElse(String.valueOf(System.currentTimeMillis()));

            Map<String, Job> jobsDisponibles = Map.of(
                    "transacciones", transaccionesDiariasJob,
                    "intereses", interesesMensualesJob,
                    "cuentas-anuales", estadosCuentaAnualesJob
            );

            if ("todos".equals(seleccion)) {
                for (Map.Entry<String, Job> entry : jobsDisponibles.entrySet()) {
                    ejecutar(jobLauncher, entry.getValue(), runId);
                }
                return;
            }

            Job job = jobsDisponibles.get(seleccion);
            if (job == null) {
                log.error("Job desconocido: '{}'. Opciones validas: {}, todos", seleccion, jobsDisponibles.keySet());
                System.exit(1);
                return;
            }
            ejecutar(jobLauncher, job, runId);
        };
    }

    private void ejecutar(JobLauncher jobLauncher, Job job, String runId) throws Exception {
        JobParameters parametros = new JobParametersBuilder()
                .addString("fechaProceso", LocalDate.now().toString())
                .addString("runId", runId)
                .toJobParameters();
        jobLauncher.run(job, parametros);
    }
}
