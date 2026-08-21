# Banco XYZ — Migración de procesos batch legacy con Spring Batch

**Experiencia 1 · Semana 2 — Desarrollo Backend III (PBY2203)**
**Actividad formativa grupal:** *Configurando jobs y steps en Spring Batch*
**Grupo:** Grupo18

## 1. Objetivo del proyecto

Este proyecto continúa el trabajo iniciado en la actividad formativa de la semana 1
("Analizando la arquitectura batch para procesar datos") y da cumplimiento a las
instrucciones específicas de la semana 2: modernizar tres procesos batch legacy del
**Banco XYZ** utilizando **Spring Batch**, garantizando integridad y consistencia de los
datos mediante buenas prácticas de gestión de errores, escalado y optimización del
rendimiento.

Se implementan tres Jobs independientes, cada uno versionado como parte del mismo
proyecto Maven:

| Job | Descripción | Fuente | Destino |
|---|---|---|---|
| **`transaccionesDiariasJob`** | Reporte de Transacciones Diarias: procesa transacciones para detectar anomalías y generar un resumen. | `data/transacciones.csv` | `transacciones_procesadas`, `resumen_transacciones_diarias` |
| **`interesesMensualesJob`** | Cálculo de Intereses Mensuales: aplica intereses sobre cuentas de ahorro y préstamo, y actualiza el saldo final. | `data/intereses.csv` | `cuentas_interes` |
| **`estadosCuentaAnualesJob`** | Generación de Estados de Cuenta Anuales: compila el historial anual por cuenta en un informe para auditorías. | `data/cuentas_anuales.csv` | `cuentas_anuales_movimientos`, `estados_cuenta_anuales` |

Los datos de origen se basan en el repositorio oficial de la actividad
[`KariVillagran/bank_legacy_data`](https://github.com/KariVillagran/bank_legacy_data)
(carpeta `data/semana_2`). El dataset oficial trae únicamente 8-10 filas por archivo —
suficiente para probar la lógica, pero no para demostrar de forma convincente el
procesamiento en chunks y multithreading que exige la actividad. Por eso:

- Los **3 CSV originales, sin modificar**, se conservan en `src/main/resources/sample-data/`.
- Se generó, con `scripts/generar_datos.py`, una **versión ampliada** (600-900 filas por
  archivo) que respeta exactamente los mismos problemas de calidad de datos descritos en el
  README del repositorio legacy (montos negativos/cero/vacíos, fechas en dos formatos,
  edades fuera de rango, tipos inválidos, registros duplicados, descripciones faltantes).
  Esta es la que usan los Jobs **por defecto**, en `src/main/resources/data/`.
- Ambas rutas son intercambiables por configuración (ver sección 8).

## 2. Arquitectura del proyecto

```
banco-xyz-batch/
├── docker-compose.yml              # PostgreSQL para desarrollo local
├── pom.xml
├── scripts/
│   └── generar_datos.py            # Generador del dataset ampliado (reproducible, seed fija)
├── src/main/resources/
│   ├── application.yml
│   ├── schema.sql                  # Tablas de negocio (metadata de Spring Batch se autogenera)
│   ├── data/                       # CSV ampliados (usados por defecto)
│   └── sample-data/                # CSV originales del repo bank_legacy_data (sin modificar)
└── src/main/java/com/bancoxyz/batch/
    ├── BancoXyzBatchApplication.java   # Punto de entrada + selector de Job por CLI
    ├── config/                          # Configuración de los 3 Jobs + infraestructura común
    │   ├── BatchProperties.java             # chunk-size, hilos, límites, rutas (externalizados)
    │   ├── InfraestructuraBatchConfig.java   # TaskExecutor (3 hilos) y JdbcTemplate
    │   ├── TransaccionesJobConfig.java
    │   ├── InteresesJobConfig.java
    │   └── CuentasAnualesJobConfig.java
    ├── model/                            # POJOs "raw" (CSV) y "procesados" (BD)
    ├── processor/                        # ItemProcessor: valida, corrige u omite
    ├── policy/                           # SkipPolicy y RetryPolicy personalizadas
    ├── listener/                         # Listeners de Job/Step/Skip + JobExecutionDecider
    ├── tasklet/                          # Tasklets: purga, resumen, agregación, revisión
    ├── exception/                        # InvalidDataException
    └── util/                             # FechaFlexibleParser (yyyy-MM-dd / yyyy/MM/dd)
```

### Flujo de cada Job

Los tres Jobs comparten el mismo patrón: un Step de preparación/carga en **chunks
paralelos y tolerantes a fallos**, seguido de un **`JobExecutionDecider`** que evalúa la
calidad de los datos cargados y deriva el flujo hacia el reporte final o hacia una revisión
manual.

```
                        ┌───────────────────┐
   (solo Jobs 1 y 3)    │  Step: purga tabla │
                        └─────────┬─────────┘
                                  ▼
                  ┌───────────────────────────────┐
                  │ Step: carga en chunks (5)      │
                  │ 3 hilos en paralelo             │
                  │ faultTolerant + skip + retry    │
                  └───────────────┬─────────────────┘
                                  ▼
                  ┌───────────────────────────────┐
                  │ ControlCalidadDecider           │
                  │ (% de registros omitidos)       │
                  └───────┬──────────────┬──────────┘
                 CALIDAD_OK           REVISION_REQUERIDA
                          ▼                      ▼
              ┌─────────────────────┐  ┌─────────────────────┐
              │ Step: resumen /      │  │ Step: revisión        │
              │ agregación final     │  │ requerida (alerta)    │
              └─────────────────────┘  └─────────────────────┘
```

## 3. Reglas de negocio por Job (validar, corregir u omitir)

Cada `ItemProcessor` decide, registro por registro, si el dato debe **corregirse**
(defecto técnico recuperable), **conservarse marcado como anomalía** (dato válido pero
sospechoso, relevante para el reporte/auditoría) u **omitirse** (irrecuperable):

**`TransaccionItemProcessor`**
- Corrige el formato de fecha legacy (`yyyy/MM/dd` → `yyyy-MM-dd`).
- Conserva y marca como anomalía: monto negativo, monto en cero, tipo no reconocido.
- Omite (skip): monto vacío o no numérico, fecha imposible de interpretar.
- Los **duplicados exactos** (misma fecha+monto+tipo) se detectan a nivel de base de datos
  (restricción `UNIQUE`) y se omiten en la escritura — ver `RegistroInvalidoSkipPolicy`.

**`InteresItemProcessor`**
- Omite: tipo de cuenta fuera de alcance (`hipoteca`, `-1`, vacío — el requerimiento solo
  contempla `ahorro` y `préstamo`), edad vacía/no numérica/fuera de rango \[18-90\], saldo
  vacío/no numérico/negativo, y registros duplicados (mismo titular+saldo+edad+tipo, aunque
  el número de cuenta difiera).
- Calcula: `ahorro` → 1.5% mensual, `préstamo` → 2.5% mensual sobre el saldo, actualizando
  `saldo_final` mediante **upsert** (`ON CONFLICT (cuenta_id) DO UPDATE`).

**`MovimientoAnualItemProcessor`**
- Corrige el formato de fecha legacy y rellena la descripción vacía con un valor por defecto.
- Corrige automáticamente el signo del monto cuando es inconsistente con el tipo de
  movimiento (un depósito no puede ser negativo; un retiro/compra no puede ser positivo) y
  deja la anomalía registrada para auditoría.
- Omite: tipo de movimiento no reconocido, monto vacío/no numérico/en cero.

## 4. Escalado y procesamiento paralelo

Tal como exigen las instrucciones específicas, los tres Steps de carga usan:

- **Chunk de tamaño 5**: `batch.chunk-size=5` (`application.yml`), aplicado con
  `.chunk(propiedades.getChunkSize(), transactionManager)`.
- **3 hilos de ejecución paralela**: `ThreadPoolTaskExecutor` con `corePoolSize = maxPoolSize
  = 3` (`InfraestructuraBatchConfig`), asignado al Step con `.taskExecutor(...)`.
- El `FlatFileItemReader` (no thread-safe) se envuelve en `SynchronizedItemStreamReader`
  para permitir lectura concurrente segura sin perder el paralelismo real, que ocurre en el
  procesamiento y la escritura de cada chunk.

## 5. Tolerancia a fallos y políticas personalizadas

- **`RegistroInvalidoSkipPolicy`** (`SkipPolicy` personalizada): decide qué excepciones se
  omiten (`InvalidDataException`, `FlatFileParseException`, `DataIntegrityViolationException`
  por duplicados) y cuáles no (p. ej. una caída de conexión a la base de datos, que debe
  fallar el Step), respetando un límite máximo configurable de omisiones
  (`batch.limite-omisiones`).
- **`ConexionTransitoriaRetryPolicy`** (`RetryPolicy` personalizada, extiende
  `SimpleRetryPolicy`): reintenta únicamente fallas transitorias de infraestructura
  (`TransientDataAccessException`, `SQLTransientException`, `QueryTimeoutException`), hasta
  `batch.maximo-reintentos` intentos, dejando cada intento registrado en el log.
- **`ControlCalidadDecider`** (`JobExecutionDecider`): política de finalización que deriva el
  flujo del Job según el porcentaje de registros omitidos (`batch.umbral-calidad-porcentaje`,
  20% por defecto).
- **Re-ejecución**: el `JobRepository` (tablas `BATCH_JOB_*`, autogeneradas por
  `spring.batch.jdbc.initialize-schema=always`) persiste el estado de cada ejecución. Para
  reintentar la misma instancia de Job (por ejemplo, tras corregir el archivo de origen) se
  puede fijar explícitamente el parámetro `runId`:
  ```bash
  java -jar target/banco-xyz-batch-1.0.0.jar transacciones --runId=2026-08-21-01
  ```
  Si se omite `--runId`, cada ejecución usa un `runId` nuevo (timestamp), creando una nueva
  instancia de Job — útil para corridas independientes repetibles.
- **`RegistroOmitidoListener`** (`SkipListener`) y **`RendimientoStepListener`**
  (`StepExecutionListener`) garantizan que ningún Step se detenga por errores de datos y
  dejan trazabilidad completa de cada omisión.

## 6. Optimización de recursos del sistema

- **Pool de conexiones (HikariCP)**: `maximum-pool-size: 8`, dimensionado para cubrir los 3
  hilos de procesamiento paralelo más margen para el `JobRepository` y los Tasklets de
  resumen, sin sobredimensionar y agotar conexiones del servidor.
- **`ThreadPoolTaskExecutor`** con `corePoolSize == maxPoolSize == 3` y `queueCapacity`
  acotada: el número de hilos activos nunca supera el dimensionado explícito, evitando
  saturar la base de datos (práctica recomendada desde Spring Batch 5, en vez del
  `throttleLimit` ya deprecado).
- Todos los parámetros clave (`chunk-size`, `hilos`, `limite-omisiones`,
  `maximo-reintentos`, `umbral-calidad-porcentaje`, rutas de los CSV) están externalizados en
  `application.yml` bajo el prefijo `batch`, y pueden ajustarse en caliente vía argumentos de
  línea de comandos sin recompilar.

## 7. Logs para evaluar rendimiento

El patrón de log (`application.yml`) incluye `[%thread]`, lo que permite ver en la consola
cómo los hilos `Batch-Thread-1/2/3` procesan chunks en paralelo. Además:

- `RendimientoStepListener` reporta, al cerrar cada Step: registros leídos/escritos/omitidos
  (desglosados por lectura/proceso/escritura), cantidad de commits (chunks) y *throughput*
  (items/segundo).
- `JobResumenListener` imprime un resumen consolidado al iniciar y finalizar cada Job.
- `ControlCalidadDecider` registra el porcentaje de omisión calculado y la rama del flujo
  elegida.

Estos logs son la base para decidir si el tamaño de chunk o el número de hilos deben
ajustarse ante un aumento del volumen de datos.

## 8. Cómo ejecutar el proyecto

### Prerrequisitos

- JDK 21
- Maven 3.9+
- Docker (para levantar PostgreSQL) — o una instancia PostgreSQL propia

### Paso a paso

```bash
# 1. Levantar PostgreSQL
docker compose up -d

# 2. Compilar
mvn clean package

# 3. Ejecutar un Job (las tablas se crean automáticamente en el primer arranque)
java -jar target/banco-xyz-batch-1.0.0.jar transacciones
java -jar target/banco-xyz-batch-1.0.0.jar intereses
java -jar target/banco-xyz-batch-1.0.0.jar cuentas-anuales

# ...o los tres en secuencia:
java -jar target/banco-xyz-batch-1.0.0.jar todos
```

También se puede ejecutar directamente con `mvn spring-boot:run -Dspring-boot.run.arguments=transacciones`
durante el desarrollo.

### Ver la rama "REVISION_REQUERIDA" del decider

El dataset ampliado (`data/`) tiene una tasa de error controlada (~15%), por debajo del
umbral de 20%, por lo que normalmente el flujo sigue el camino `CALIDAD_OK`. Para ver la
rama alterna en acción, se puede apuntar un Job al dataset original y pequeño (con ~60% de
registros inválidos en `intereses.csv`):

```bash
java -jar target/banco-xyz-batch-1.0.0.jar intereses \
  --batch.rutas.intereses=classpath:sample-data/intereses.csv
```

### Verificar los resultados en la base de datos

```bash
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM resumen_transacciones_diarias;"
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM cuentas_interes LIMIT 10;"
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM estados_cuenta_anuales LIMIT 10;"
```

## 9. Evidencia de ejecución

La carpeta [`evidencias/`](evidencias/) está preparada para alojar las capturas de pantalla
y/o salidas de consola de cada Job, tal como exige la entrega. Sugerencia de contenido
mínimo por Job: (a) el comando ejecutado, (b) el log completo desde `INICIO JOB` hasta
`FIN JOB` con el resumen de `RendimientoStepListener`, y (c) una consulta SQL mostrando los
datos ya escritos en la tabla destino.

## 10. Cómo subir este proyecto a tu cuenta de GitHub

Este proyecto ya quedó inicializado como repositorio Git local (ver `git log`). Para
publicarlo en tu propia cuenta:

```bash
# 1. Crea un repositorio vacío en GitHub (sin README/licencia) llamado, por ejemplo,
#    "banco-xyz-batch", desde https://github.com/new

# 2. Desde la carpeta del proyecto:
git remote add origin https://github.com/<tu-usuario>/banco-xyz-batch.git
git branch -M main
git push -u origin main
```

## 11. Trazabilidad con la pauta de evaluación formativa

| Criterio de la pauta | Dónde se implementa |
|---|---|
| Configura los Jobs y Steps para manejar grandes volúmenes de datos de manera eficiente | 3 Jobs multi-Step (`config/*JobConfig.java`), dataset ampliado (`scripts/generar_datos.py`), lectura por streaming con `FlatFileItemReader` |
| Escala el procesamiento y configura procesamiento paralelo (chunks, multithreading) | `chunk(5, ...)` + `ThreadPoolTaskExecutor` de 3 hilos + `SynchronizedItemStreamReader` (sección 4) |
| Implementa políticas de finalización y re-ejecución, controlando reintentos y omisiones | `ControlCalidadDecider`, `RegistroInvalidoSkipPolicy`, `ConexionTransitoriaRetryPolicy`, `JobRepository` + `runId` (sección 5) |
| Optimiza los recursos del sistema configurando parámetros | `BatchProperties`, HikariCP, `ThreadPoolTaskExecutor` dimensionado (sección 6) |
| Maneja errores y excepciones usando políticas y listeners | `RegistroInvalidoSkipPolicy`, `RegistroOmitidoListener`, `ConexionTransitoriaRetryPolicy` (sección 5) |
| Implementa técnicas de logs para evaluar el rendimiento | `RendimientoStepListener`, `JobResumenListener`, patrón `[%thread]` (sección 7) |

## 12. Nota sobre el entorno en que se preparó este proyecto

Este proyecto fue generado y revisado en un entorno sandbox sin acceso a Maven Central ni a
un daemon Docker, por lo que **no pudo compilarse ni ejecutarse dentro de ese entorno**. Cada
firma de API utilizada (Spring Batch 5.1.2 / Spring Retry, las versiones exactas que trae
`spring-boot-starter-parent:3.3.4`) fue verificada línea por línea contra el código fuente
oficial de esas librerías antes de escribirse aquí, pero se recomienda como primer paso
ejecutar `mvn clean compile` en un entorno con acceso normal a internet y revisar la salida
antes de dar por definitiva la entrega.
