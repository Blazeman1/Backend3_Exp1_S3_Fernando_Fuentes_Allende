package com.bancoxyz.batch.config;

import com.bancoxyz.batch.listener.ControlCalidadDecider;
import com.bancoxyz.batch.listener.JobResumenListener;
import com.bancoxyz.batch.listener.RegistroOmitidoListener;
import com.bancoxyz.batch.listener.RendimientoStepListener;
import com.bancoxyz.batch.model.MovimientoAnualProcesado;
import com.bancoxyz.batch.model.MovimientoAnualRaw;
import com.bancoxyz.batch.policy.ConexionTransitoriaRetryPolicy;
import com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy;
import com.bancoxyz.batch.processor.MovimientoAnualItemProcessor;
import com.bancoxyz.batch.tasklet.AgregacionEstadosCuentaTasklet;
import com.bancoxyz.batch.tasklet.PurgaTablaTasklet;
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
 * Job 3: Generacion de Estados de Cuenta Anuales.
 *
 * <p>Flujo: {@code purgaMovimientosAnualesStep} -&gt; {@code cargaMovimientosAnualesStep}
 * (chunk, 3 hilos, chunk=5, tolerante a fallos) -&gt; {@code controlCalidadCuentasAnualesDecider}
 * -&gt; {@code agregacionEstadosCuentaStep} (compila el informe detallado por cuenta para
 * auditorias, tal como piden las instrucciones especificas) o {@code revisionCuentasAnualesStep}.</p>
 */
@Configuration
public class CuentasAnualesJobConfig {

    @Bean
    public FlatFileItemReader<MovimientoAnualRaw> cuentasAnualesFlatFileReader(
            @Value("${batch.rutas.cuentas-anuales}") Resource resource) {
        return new FlatFileItemReaderBuilder<MovimientoAnualRaw>()
                .name("cuentasAnualesFlatFileReader")
                .resource(resource)
                // UTF-8 explicito: el dataset oficial de la semana 3 trae el tipo de movimiento
                // "depósito" con tilde (ver MovimientoAnualItemProcessor), y dejar el encoding
                // librado al default de la plataforma es justamente el tipo de bug silencioso
                // que solo aparece al ejecutar en otra maquina/CI.
                .encoding("UTF-8")
                .linesToSkip(1)
                .delimited()
                .names("cuentaId", "fecha", "transaccion", "monto", "descripcion")
                .targetType(MovimientoAnualRaw.class)
                .strict(true)
                .build();
    }

    @Bean
    public SynchronizedItemStreamReader<MovimientoAnualRaw> cuentasAnualesReader(
            FlatFileItemReader<MovimientoAnualRaw> cuentasAnualesFlatFileReader) {
        return new SynchronizedItemStreamReaderBuilder<MovimientoAnualRaw>()
                .delegate(cuentasAnualesFlatFileReader)
                .build();
    }

    @Bean
    public MovimientoAnualItemProcessor movimientoAnualItemProcessor() {
        return new MovimientoAnualItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<MovimientoAnualProcesado> movimientosAnualesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<MovimientoAnualProcesado>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO cuentas_anuales_movimientos
                            (cuenta_id, fecha, tipo_movimiento, monto, descripcion, es_anomalia, motivo_anomalia)
                        VALUES
                            (:cuentaId, :fecha, :tipoMovimiento, :monto, :descripcion, :esAnomalia, :motivoAnomalia)
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public ControlCalidadDecider controlCalidadCuentasAnualesDecider(BatchProperties propiedades) {
        return new ControlCalidadDecider(propiedades.getUmbralCalidadPorcentaje());
    }

    @Bean
    public Step purgaMovimientosAnualesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                             JdbcTemplate jdbcTemplate) {
        return new StepBuilder("purgaMovimientosAnualesStep", jobRepository)
                .tasklet(new PurgaTablaTasklet(jdbcTemplate, "cuentas_anuales_movimientos"), transactionManager)
                .build();
    }

    @Bean
    public Step cargaMovimientosAnualesStep(JobRepository jobRepository,
                                             PlatformTransactionManager transactionManager,
                                             SynchronizedItemStreamReader<MovimientoAnualRaw> cuentasAnualesReader,
                                             MovimientoAnualItemProcessor movimientoAnualItemProcessor,
                                             JdbcBatchItemWriter<MovimientoAnualProcesado> movimientosAnualesWriter,
                                             TaskExecutor batchTaskExecutor,
                                             BatchProperties propiedades) {
        return new StepBuilder("cargaMovimientosAnualesStep", jobRepository)
                .<MovimientoAnualRaw, MovimientoAnualProcesado>chunk(propiedades.getChunkSize(), transactionManager)
                .reader(cuentasAnualesReader)
                .processor(movimientoAnualItemProcessor)
                .writer(movimientosAnualesWriter)
                .faultTolerant()
                .skipPolicy(new RegistroInvalidoSkipPolicy("cargaMovimientosAnualesStep", propiedades.getLimiteOmisiones()))
                .retryPolicy(new ConexionTransitoriaRetryPolicy("cargaMovimientosAnualesStep", propiedades.getMaximoReintentos()))
                .listener(new RegistroOmitidoListener("cargaMovimientosAnualesStep"))
                .listener(new RendimientoStepListener())
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step agregacionEstadosCuentaStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                             JdbcTemplate jdbcTemplate) {
        return new StepBuilder("agregacionEstadosCuentaStep", jobRepository)
                .tasklet(new AgregacionEstadosCuentaTasklet(jdbcTemplate), transactionManager)
                .build();
    }

    @Bean
    public Step revisionCuentasAnualesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("revisionCuentasAnualesStep", jobRepository)
                .tasklet(new RevisionRequeridaTasklet("Generacion de Estados de Cuenta Anuales"), transactionManager)
                .build();
    }

    @Bean
    public Job estadosCuentaAnualesJob(JobRepository jobRepository,
                                        Step purgaMovimientosAnualesStep,
                                        Step cargaMovimientosAnualesStep,
                                        Step agregacionEstadosCuentaStep,
                                        Step revisionCuentasAnualesStep,
                                        ControlCalidadDecider controlCalidadCuentasAnualesDecider) {
        return new JobBuilder("estadosCuentaAnualesJob", jobRepository)
                .listener(new JobResumenListener())
                .start(purgaMovimientosAnualesStep)
                .next(cargaMovimientosAnualesStep)
                .next(controlCalidadCuentasAnualesDecider)
                    .on(ControlCalidadDecider.REVISION_REQUERIDA).to(revisionCuentasAnualesStep)
                .from(controlCalidadCuentasAnualesDecider)
                    .on(ControlCalidadDecider.CALIDAD_OK).to(agregacionEstadosCuentaStep)
                .end()
                .build();
    }
}
