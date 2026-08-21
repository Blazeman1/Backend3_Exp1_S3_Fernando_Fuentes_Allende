package com.bancoxyz.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

/**
 * Configuracion transversal a los tres Jobs: el {@link TaskExecutor} usado por los Steps
 * multithread y el {@link JdbcTemplate} usado por los Tasklets de resumen/agregacion.
 *
 * <p>El {@code ThreadPoolTaskExecutor} se dimensiona con {@code corePoolSize == maxPoolSize
 * == 3} (exactamente los 3 hilos de ejecucion paralela exigidos por las instrucciones
 * especificas). Fijar core y max al mismo valor, en vez de dejar un rango con
 * {@code throttleLimit}, es la practica recomendada desde Spring Batch 5: el propio pool de
 * hilos ya actua como limite duro de concurrencia, sin necesidad de una configuracion
 * adicional (y hoy deprecada) en el Step. Esto es, ademas, la forma mas directa de
 * "optimizar los recursos del sistema": el numero de hilos activos nunca puede superar el
 * dimensionado explicito del pool, evitando saturar la base de datos con mas conexiones
 * concurrentes de las que el {@code HikariCP} tiene disponibles (ver application.yml).</p>
 */
@Configuration
@EnableConfigurationProperties(BatchProperties.class)
public class InfraestructuraBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(InfraestructuraBatchConfig.class);

    @Bean
    public TaskExecutor batchTaskExecutor(BatchProperties propiedades) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(propiedades.getHilos());
        executor.setMaxPoolSize(propiedades.getHilos());
        // Cola pequena y acotada: si se llena, es preferible que el hilo que envia la tarea
        // espere (backpressure) a que se acumulen tareas sin control y se dispare el consumo de memoria.
        executor.setQueueCapacity(propiedades.getHilos() * 5);
        executor.setThreadNamePrefix("Batch-Thread-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        log.info("TaskExecutor inicializado: corePoolSize={} maxPoolSize={} queueCapacity={}",
                propiedades.getHilos(), propiedades.getHilos(), propiedades.getHilos() * 5);
        return executor;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
