package com.bancoxyz.batch.config;

import com.bancoxyz.batch.listener.ControlCalidadDecider;
import com.bancoxyz.batch.listener.JobResumenListener;
import com.bancoxyz.batch.listener.RegistroOmitidoListener;
import com.bancoxyz.batch.listener.RendimientoStepListener;
import com.bancoxyz.batch.model.TransaccionProcesada;
import com.bancoxyz.batch.model.TransaccionRaw;
import com.bancoxyz.batch.partition.TransaccionesRangoPartitioner;
import com.bancoxyz.batch.policy.ConexionTransitoriaRetryPolicy;
import com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy;
import com.bancoxyz.batch.processor.TransaccionItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Semana 3 (actividad sumativa individual): modo ALTERNATIVO de escalamiento para el Job de
 * transacciones diarias, basado en <b>particionado real</b> (Partitioner + PartitionHandler)
 * en vez de -o ademas de- el Step multi-hilo de {@link TransaccionesJobConfig}.
 *
 * <p><b>Por que un archivo de configuracion separado y no modificar {@code TransaccionesJobConfig}:</b>
 * el Job {@code transaccionesDiariasJob} (Step {@code cargaTransaccionesStep} multi-hilo) ya fue
 * validado exhaustivamente con evidencia real de ejecucion en la Semana 2 (incluyendo la
 * correccion de tres bugs reales). Tocar ese archivo para agregar particionado arriesgaria
 * regresiones sobre codigo ya probado. En su lugar, este archivo agrega un Job COMPLETAMENTE
 * NUEVO ({@code transaccionesDiariasParticionadoJob}) que reutiliza los Steps/beans no-invasivos
 * del Job original (purga, resumen, revision, decider de calidad) y solo reemplaza el Step de
 * carga por su version particionada. Ambos Jobs conviven en el mismo contexto de Spring y se
 * seleccionan por linea de comandos (ver {@code BancoXyzBatchApplication}), lo que permite
 * comparar ambas estrategias de escalamiento con el mismo dataset de entrada (criterio de la
 * pauta S3: "Implementa politicas de escalamiento... particiones... comparando diferentes
 * parametros para encontrar la configuracion optima").</p>
 *
 * <h3>Arquitectura de particionado</h3>
 * <ul>
 *   <li>{@link TransaccionesRangoPartitioner}: cuenta las filas del CSV y las reparte en
 *       {@code batch.grid-size} rangos [start, end], uno por particion.</li>
 *   <li>{@code transaccionParticionReader}: un {@link FlatFileItemReader} de alcance
 *       {@code @StepScope} (una instancia NUEVA por cada ejecucion de particion, cada una abre
 *       el archivo por su cuenta) que salta directamente a su rango asignado usando
 *       {@code setCurrentItemCount}/{@code setMaxItemCount}. Esto funciona porque
 *       {@code AbstractItemCountingItemStreamItemReader.open()} - al no encontrar todavia, en el
 *       {@code ExecutionContext} propio y nuevo de esta particion, una clave de restart
 *       ("read.count") - usa el {@code currentItemCount} que fijamos antes de abrir el reader
 *       para saltar (via {@code jumpToItem}, lectura eficiente linea por linea) directo hasta el
 *       inicio de su rango asignado.</li>
 *   <li>{@code workerCargaTransaccionesStep}: el Step "minion" - un Step chunk-oriented normal,
 *       SIN taskExecutor propio (el paralelismo ya lo da el hecho de correr varias instancias de
 *       este Step al mismo tiempo, una por particion), que reutiliza el mismo
 *       {@code ItemProcessor}/{@code ItemWriter}/politicas de skip-retry ya validados en la
 *       Semana 2.</li>
 *   <li>{@code particionTaskExecutor} + {@code particionTransaccionesHandler}
 *       ({@link TaskExecutorPartitionHandler}): el {@code PartitionHandler} que efectivamente
 *       lanza las {@code gridSize} ejecuciones del Step minion en paralelo.</li>
 *   <li>{@code particionTransaccionesStep}: el Step "manager" (un {@code PartitionStep}) que
 *       orquesta el particionado y agrega los resultados de todas las particiones antes de
 *       continuar el flujo del Job.</li>
 * </ul>
 */
@Configuration
public class TransaccionesParticionadoJobConfig {

    private static final Logger log = LoggerFactory.getLogger(TransaccionesParticionadoJobConfig.class);

    @Bean
    public TransaccionesRangoPartitioner transaccionesRangoPartitioner(
            @Value("${batch.rutas.transacciones}") Resource resource) {
        return new TransaccionesRangoPartitioner(resource);
    }

    /**
     * Reader de alcance Step: Spring Batch crea una instancia nueva de este bean por cada
     * ejecucion de particion, inyectando el {@code start}/{@code end} que el Partitioner dejo en
     * el {@code ExecutionContext} de ESA particion especifica.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<TransaccionRaw> transaccionParticionReader(
            @Value("${batch.rutas.transacciones}") Resource resource,
            @Value("#{stepExecutionContext['start']}") Long start,
            @Value("#{stepExecutionContext['end']}") Long end) {
        FlatFileItemReader<TransaccionRaw> reader = new FlatFileItemReaderBuilder<TransaccionRaw>()
                .name("transaccionParticionReader")
                .resource(resource)
                .linesToSkip(1)
                .delimited()
                .names("id", "fecha", "monto", "tipo")
                .targetType(TransaccionRaw.class)
                .strict(true)
                .build();
        // saveState se deja en su valor por defecto (true) A PROPOSITO: es lo que hace que
        // AbstractItemCountingItemStreamItemReader.open() invoque jumpToItem(itemCount) usando
        // el currentItemCount que fijamos aqui abajo (al no existir todavia una clave
        // "transaccionParticionReader.read.count" en el ExecutionContext propio y nuevo de esta
        // particion, el metodo usa nuestro valor inicial en su lugar). De paso, esto deja cada
        // particion correctamente reanudable de forma independiente si el Job fallara a mitad
        // de camino y se reintentara la misma instancia.
        reader.setCurrentItemCount(Math.toIntExact(start - 1));
        reader.setMaxItemCount(Math.toIntExact(end));
        log.debug("transaccionParticionReader configurado: filas {} a {}", start, end);
        return reader;
    }

    /**
     * Step "minion": procesa el rango de filas asignado por el Partitioner. No lleva
     * {@code taskExecutor} propio -corre secuencialmente dentro de su propio hilo de particion-
     * porque el paralelismo real lo aporta el {@code PartitionHandler} al correr varias
     * instancias de este Step en simultaneo.
     */
    @Bean
    public Step workerCargaTransaccionesStep(JobRepository jobRepository,
                                              PlatformTransactionManager transactionManager,
                                              FlatFileItemReader<TransaccionRaw> transaccionParticionReader,
                                              TransaccionItemProcessor transaccionItemProcessor,
                                              JdbcBatchItemWriter<TransaccionProcesada> transaccionesWriter,
                                              BatchProperties propiedades) {
        return new StepBuilder("workerCargaTransaccionesStep", jobRepository)
                .<TransaccionRaw, TransaccionProcesada>chunk(propiedades.getChunkSize(), transactionManager)
                .reader(transaccionParticionReader)
                .processor(transaccionItemProcessor)
                .writer(transaccionesWriter)
                .faultTolerant()
                .skipPolicy(new RegistroInvalidoSkipPolicy("workerCargaTransaccionesStep", propiedades.getLimiteOmisiones()))
                .retryPolicy(new ConexionTransitoriaRetryPolicy("workerCargaTransaccionesStep", propiedades.getMaximoReintentos()))
                .listener(new RegistroOmitidoListener("workerCargaTransaccionesStep"))
                .listener(new RendimientoStepListener())
                .build();
    }

    /**
     * TaskExecutor dedicado al particionado, dimensionado segun {@code batch.grid-size} (no se
     * reutiliza el {@code batchTaskExecutor} de {@link InfraestructuraBatchConfig}, fijo en 3
     * hilos para el modo multi-thread, porque aqui el numero de particiones es justamente el
     * parametro que el benchmark de la Semana 3 hace variar).
     */
    @Bean
    public TaskExecutor particionTaskExecutor(BatchProperties propiedades) {
        int hilos = Math.max(propiedades.getGridSize(), 1);
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(hilos);
        executor.setMaxPoolSize(hilos);
        executor.setQueueCapacity(hilos * 2);
        executor.setThreadNamePrefix("Particion-Thread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        log.info("particionTaskExecutor inicializado: corePoolSize={} maxPoolSize={} (gridSize={})",
                hilos, hilos, propiedades.getGridSize());
        return executor;
    }

    @Bean
    public PartitionHandler particionTransaccionesHandler(TaskExecutor particionTaskExecutor,
                                                            Step workerCargaTransaccionesStep,
                                                            BatchProperties propiedades) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(particionTaskExecutor);
        handler.setStep(workerCargaTransaccionesStep);
        handler.setGridSize(propiedades.getGridSize());
        return handler;
    }

    /**
     * Step "manager": no procesa datos el mismo, orquesta el particionado (crea las
     * particiones via el Partitioner, delega su ejecucion paralela al PartitionHandler y agrega
     * los StepExecution resultantes en uno solo antes de continuar el flujo del Job).
     */
    @Bean
    public Step particionTransaccionesStep(JobRepository jobRepository,
                                            TransaccionesRangoPartitioner transaccionesRangoPartitioner,
                                            PartitionHandler particionTransaccionesHandler) {
        return new StepBuilder("particionTransaccionesStep", jobRepository)
                .partitioner("workerCargaTransaccionesStep", transaccionesRangoPartitioner)
                .partitionHandler(particionTransaccionesHandler)
                .build();
    }

    /**
     * Job alternativo de transacciones diarias: identico en flujo de negocio a
     * {@code transaccionesDiariasJob} (purga -&gt; carga -&gt; control de calidad -&gt; resumen o
     * revision), reutilizando esos mismos Steps/decider, pero con {@code particionTransaccionesStep}
     * (particionado) en lugar de {@code cargaTransaccionesStep} (multi-hilo) como Step de carga.
     */
    @Bean
    public Job transaccionesDiariasParticionadoJob(JobRepository jobRepository,
                                                    Step purgaTransaccionesStep,
                                                    Step particionTransaccionesStep,
                                                    Step resumenTransaccionesStep,
                                                    Step revisionTransaccionesStep,
                                                    ControlCalidadDecider controlCalidadTransaccionesDecider) {
        return new JobBuilder("transaccionesDiariasParticionadoJob", jobRepository)
                .listener(new JobResumenListener())
                .start(purgaTransaccionesStep)
                .next(particionTransaccionesStep)
                .next(controlCalidadTransaccionesDecider)
                    .on(ControlCalidadDecider.REVISION_REQUERIDA).to(revisionTransaccionesStep)
                        // Ver comentario detallado en TransaccionesJobConfig: sin esta transicion
                        // explicita, el Step colgante revisionTransaccionesStep hereda la regla por
                        // defecto de FlowBuilder (on("COMPLETED").end() + on("*").fail()) y, como su
                        // ExitStatus real es "REVISION_REQUERIDA" (no "COMPLETED"), el Job terminaba
                        // con BatchStatus.FAILED en vez de completar normalmente con ese exit status
                        // informativo (confirmado con evidencia real de ejecucion: este Job comparte
                        // el mismo revisionTransaccionesStep que transaccionesDiariasJob y por eso
                        // tenia exactamente el mismo bug).
                        .on("*").end(ControlCalidadDecider.REVISION_REQUERIDA)
                .from(controlCalidadTransaccionesDecider)
                    .on(ControlCalidadDecider.CALIDAD_OK).to(resumenTransaccionesStep)
                .end()
                .build();
    }
}
