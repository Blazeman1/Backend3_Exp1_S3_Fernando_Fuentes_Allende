package com.bancoxyz.batch.config;

import com.bancoxyz.batch.listener.ControlCalidadDecider;
import com.bancoxyz.batch.listener.JobResumenListener;
import com.bancoxyz.batch.listener.RegistroOmitidoListener;
import com.bancoxyz.batch.listener.RendimientoStepListener;
import com.bancoxyz.batch.model.TransaccionProcesada;
import com.bancoxyz.batch.model.TransaccionRaw;
import com.bancoxyz.batch.policy.ConexionTransitoriaRetryPolicy;
import com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy;
import com.bancoxyz.batch.processor.TransaccionItemProcessor;
import com.bancoxyz.batch.tasklet.PurgaTablaTasklet;
import com.bancoxyz.batch.tasklet.ResumenTransaccionesDiariasTasklet;
import com.bancoxyz.batch.tasklet.RevisionRequeridaTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Job 1: Reporte de Transacciones Diarias.
 *
 * <p>Flujo: {@code purgaTransaccionesStep} (limpia la tabla destino) -&gt;
 * {@code cargaTransaccionesStep} (Step chunk-oriented, 3 hilos en paralelo, chunk=5, tolerante
 * a fallos) -&gt; {@code controlCalidadTransaccionesDecider} (evalua % de omision) -&gt;
 * {@code resumenTransaccionesStep} (genera el resumen ejecutivo) o
 * {@code revisionTransaccionesStep} (si la calidad de datos no es suficiente).</p>
 */
@Configuration
public class TransaccionesJobConfig {

    @Bean
    public FlatFileItemReader<TransaccionRaw> transaccionesFlatFileReader(
            @Value("${batch.rutas.transacciones}") Resource resource) {
        return new FlatFileItemReaderBuilder<TransaccionRaw>()
                .name("transaccionesFlatFileReader")
                .resource(resource)
                .linesToSkip(1)
                .delimited()
                .names("id", "fecha", "monto", "tipo")
                .targetType(TransaccionRaw.class)
                .strict(true)
                .build();
    }

    /**
     * {@code FlatFileItemReader} no es thread-safe: varios hilos no pueden invocar {@code read()}
     * concurrentemente sobre la misma instancia. Al envolverlo en {@link SynchronizedItemStreamReader}
     * se sincroniza el acceso de lectura (una operacion muy rapida) sin perder el paralelismo real,
     * que ocurre en las etapas de procesamiento y escritura de cada chunk.
     */
    @Bean
    public SynchronizedItemStreamReader<TransaccionRaw> transaccionesReader(
            FlatFileItemReader<TransaccionRaw> transaccionesFlatFileReader) {
        return new SynchronizedItemStreamReaderBuilder<TransaccionRaw>()
                .delegate(transaccionesFlatFileReader)
                .build();
    }

    @Bean
    public TransaccionItemProcessor transaccionItemProcessor() {
        return new TransaccionItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<TransaccionProcesada> transaccionesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<TransaccionProcesada>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO transacciones_procesadas (id_origen, fecha, monto, tipo, es_anomalia, motivo_anomalia)
                        VALUES (:idOrigen, :fecha, :monto, :tipo, :esAnomalia, :motivoAnomalia)
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public ControlCalidadDecider controlCalidadTransaccionesDecider(BatchProperties propiedades) {
        return new ControlCalidadDecider(propiedades.getUmbralCalidadPorcentaje());
    }

    @Bean
    public Step purgaTransaccionesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                        JdbcTemplate jdbcTemplate) {
        return new StepBuilder("purgaTransaccionesStep", jobRepository)
                .tasklet(new PurgaTablaTasklet(jdbcTemplate, "transacciones_procesadas"), transactionManager)
                .build();
    }

    /**
     * Step principal: procesamiento en chunks de tamano {@code batch.chunk-size} (5) con
     * {@code batch.hilos} (3) hilos de ejecucion paralela, tolerancia a fallos habilitada con
     * politicas de omision y reintento personalizadas, y listeners de omision/rendimiento.
     */
    @Bean
    public Step cargaTransaccionesStep(JobRepository jobRepository,
                                        PlatformTransactionManager transactionManager,
                                        SynchronizedItemStreamReader<TransaccionRaw> transaccionesReader,
                                        TransaccionItemProcessor transaccionItemProcessor,
                                        JdbcBatchItemWriter<TransaccionProcesada> transaccionesWriter,
                                        TaskExecutor batchTaskExecutor,
                                        BatchProperties propiedades) {
        return new StepBuilder("cargaTransaccionesStep", jobRepository)
                .<TransaccionRaw, TransaccionProcesada>chunk(propiedades.getChunkSize(), transactionManager)
                .reader(transaccionesReader)
                .processor(transaccionItemProcessor)
                .writer(transaccionesWriter)
                .faultTolerant()
                .skipPolicy(new RegistroInvalidoSkipPolicy("cargaTransaccionesStep", propiedades.getLimiteOmisiones()))
                .retryPolicy(new ConexionTransitoriaRetryPolicy("cargaTransaccionesStep", propiedades.getMaximoReintentos()))
                .listener(new RegistroOmitidoListener("cargaTransaccionesStep"))
                .listener(new RendimientoStepListener())
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step resumenTransaccionesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                          JdbcTemplate jdbcTemplate) {
        return new StepBuilder("resumenTransaccionesStep", jobRepository)
                .tasklet(new ResumenTransaccionesDiariasTasklet(jdbcTemplate), transactionManager)
                .build();
    }

    @Bean
    public Step revisionTransaccionesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("revisionTransaccionesStep", jobRepository)
                .tasklet(new RevisionRequeridaTasklet("Reporte de Transacciones Diarias"), transactionManager)
                .build();
    }

    @Bean
    public Job transaccionesDiariasJob(JobRepository jobRepository,
                                        Step purgaTransaccionesStep,
                                        Step cargaTransaccionesStep,
                                        Step resumenTransaccionesStep,
                                        Step revisionTransaccionesStep,
                                        ControlCalidadDecider controlCalidadTransaccionesDecider) {
        return new JobBuilder("transaccionesDiariasJob", jobRepository)
                .listener(new JobResumenListener())
                .start(purgaTransaccionesStep)
                .next(cargaTransaccionesStep)
                .next(controlCalidadTransaccionesDecider)
                    .on(ControlCalidadDecider.REVISION_REQUERIDA).to(revisionTransaccionesStep)
                .from(controlCalidadTransaccionesDecider)
                    .on(ControlCalidadDecider.CALIDAD_OK).to(resumenTransaccionesStep)
                .end()
                .build();
    }
}
