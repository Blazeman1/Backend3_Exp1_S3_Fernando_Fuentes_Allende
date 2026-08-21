package com.bancoxyz.batch.config;

import com.bancoxyz.batch.listener.ControlCalidadDecider;
import com.bancoxyz.batch.listener.JobResumenListener;
import com.bancoxyz.batch.listener.RegistroOmitidoListener;
import com.bancoxyz.batch.listener.RendimientoStepListener;
import com.bancoxyz.batch.model.CuentaInteresProcesada;
import com.bancoxyz.batch.model.CuentaInteresRaw;
import com.bancoxyz.batch.policy.ConexionTransitoriaRetryPolicy;
import com.bancoxyz.batch.policy.RegistroInvalidoSkipPolicy;
import com.bancoxyz.batch.processor.InteresItemProcessor;
import com.bancoxyz.batch.tasklet.RevisionRequeridaTasklet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Job 2: Calculo de Intereses Mensuales.
 *
 * <p>A diferencia del Job de transacciones, la tabla destino {@code cuentas_interes} tiene una
 * clave de negocio natural ({@code cuenta_id}), por lo que el Step de carga usa un
 * {@code INSERT ... ON CONFLICT DO UPDATE} (upsert) y no requiere un Step previo de purga: cada
 * re-ejecucion simplemente actualiza el saldo final de cada cuenta con la tasa vigente.</p>
 *
 * <p>Flujo: {@code calculoInteresesStep} (chunk, 3 hilos, chunk=5, tolerante a fallos) -&gt;
 * {@code controlCalidadInteresesDecider} -&gt; {@code confirmacionInteresesStep} (registra el
 * cierre exitoso) o {@code revisionInteresesStep}.</p>
 */
@Configuration
public class InteresesJobConfig {

    private static final Logger log = LoggerFactory.getLogger(InteresesJobConfig.class);

    @Bean
    public FlatFileItemReader<CuentaInteresRaw> interesesFlatFileReader(
            @Value("${batch.rutas.intereses}") Resource resource) {
        return new FlatFileItemReaderBuilder<CuentaInteresRaw>()
                .name("interesesFlatFileReader")
                .resource(resource)
                .linesToSkip(1)
                .delimited()
                .names("cuentaId", "nombre", "saldo", "edad", "tipo")
                .targetType(CuentaInteresRaw.class)
                .strict(true)
                .build();
    }

    @Bean
    public SynchronizedItemStreamReader<CuentaInteresRaw> interesesReader(
            FlatFileItemReader<CuentaInteresRaw> interesesFlatFileReader) {
        return new SynchronizedItemStreamReaderBuilder<CuentaInteresRaw>()
                .delegate(interesesFlatFileReader)
                .build();
    }

    @Bean
    public InteresItemProcessor interesItemProcessor() {
        return new InteresItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<CuentaInteresProcesada> cuentasInteresWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<CuentaInteresProcesada>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO cuentas_interes
                            (cuenta_id, nombre, tipo, edad, saldo_inicial, tasa_interes_mensual, interes_calculado, saldo_final)
                        VALUES
                            (:cuentaId, :nombre, :tipo, :edad, :saldoInicial, :tasaInteresMensual, :interesCalculado, :saldoFinal)
                        ON CONFLICT (cuenta_id) DO UPDATE SET
                            nombre = EXCLUDED.nombre,
                            tipo = EXCLUDED.tipo,
                            edad = EXCLUDED.edad,
                            saldo_inicial = EXCLUDED.saldo_inicial,
                            tasa_interes_mensual = EXCLUDED.tasa_interes_mensual,
                            interes_calculado = EXCLUDED.interes_calculado,
                            saldo_final = EXCLUDED.saldo_final,
                            fecha_procesamiento = now()
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    public ControlCalidadDecider controlCalidadInteresesDecider(BatchProperties propiedades) {
        return new ControlCalidadDecider(propiedades.getUmbralCalidadPorcentaje());
    }

    @Bean
    public Step calculoInteresesStep(JobRepository jobRepository,
                                      PlatformTransactionManager transactionManager,
                                      SynchronizedItemStreamReader<CuentaInteresRaw> interesesReader,
                                      InteresItemProcessor interesItemProcessor,
                                      JdbcBatchItemWriter<CuentaInteresProcesada> cuentasInteresWriter,
                                      TaskExecutor batchTaskExecutor,
                                      BatchProperties propiedades) {
        return new StepBuilder("calculoInteresesStep", jobRepository)
                .<CuentaInteresRaw, CuentaInteresProcesada>chunk(propiedades.getChunkSize(), transactionManager)
                .reader(interesesReader)
                .processor(interesItemProcessor)
                .writer(cuentasInteresWriter)
                .faultTolerant()
                .skipPolicy(new RegistroInvalidoSkipPolicy("calculoInteresesStep", propiedades.getLimiteOmisiones()))
                .retryPolicy(new ConexionTransitoriaRetryPolicy("calculoInteresesStep", propiedades.getMaximoReintentos()))
                .listener(new RegistroOmitidoListener("calculoInteresesStep"))
                .listener(new RendimientoStepListener())
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step confirmacionInteresesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                           JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            Integer totalCuentas = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM cuentas_interes", Integer.class);
            log.info("Calculo de intereses mensuales finalizado. Total de cuentas con saldo actualizado: {}", totalCuentas);
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("confirmacionInteresesStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean
    public Step revisionInteresesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("revisionInteresesStep", jobRepository)
                .tasklet(new RevisionRequeridaTasklet("Calculo de Intereses Mensuales"), transactionManager)
                .build();
    }

    @Bean
    public Job interesesMensualesJob(JobRepository jobRepository,
                                      Step calculoInteresesStep,
                                      Step confirmacionInteresesStep,
                                      Step revisionInteresesStep,
                                      ControlCalidadDecider controlCalidadInteresesDecider) {
        return new JobBuilder("interesesMensualesJob", jobRepository)
                .listener(new JobResumenListener())
                .start(calculoInteresesStep)
                .next(controlCalidadInteresesDecider)
                    .on(ControlCalidadDecider.REVISION_REQUERIDA).to(revisionInteresesStep)
                .from(controlCalidadInteresesDecider)
                    .on(ControlCalidadDecider.CALIDAD_OK).to(confirmacionInteresesStep)
                .end()
                .build();
    }
}
