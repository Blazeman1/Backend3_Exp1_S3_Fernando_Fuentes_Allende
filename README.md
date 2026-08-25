# Banco XYZ — Migración de procesos batch legacy con Spring Batch

**Experiencia 1 · Semana 3 — Desarrollo Backend III (PBY2203)**
**Actividad sumativa individual:** *Optimizando procesos batch para mejorar la resiliencia de procesos*
**Alumno:** Fernando Fuentes Allende

## 1. Objetivo del proyecto

Este proyecto continúa, en su continuidad y de forma individual, el trabajo realizado en
las actividades formativas de las semanas 1 y 2 (arquitectura batch y configuración de
Jobs/Steps en Spring Batch) y da cumplimiento a las instrucciones específicas de la
semana 3: modernizar tres procesos batch legacy del **Banco XYZ** utilizando
**Spring Batch**, garantizando integridad y consistencia de los datos, e incorporando
además políticas de escalamiento adicionales (particionado real, sección 4) evaluadas
mediante un benchmark comparativo, para "encontrar la configuración óptima" del sistema.

> **Nota sobre continuidad:** todo lo descrito en las secciones 1-3 y 5-7 (arquitectura de
> los 3 Jobs, reglas de negocio, tolerancia a fallos, optimización de recursos y logs de
> rendimiento) proviene de las actividades formativas previas y ya fue validado con
> evidencia real de ejecución; se mantiene sin cambios de diseño. Lo nuevo de la semana 3
> es exclusivamente el modo de particionado y el benchmark comparativo de la sección 4.

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
├── .github/workflows/
│   └── evidencia-ejecucion.yml     # CI: compila y corre los 3 Jobs, publica los logs como artefacto
├── docker-compose.yml              # PostgreSQL para desarrollo local
├── pom.xml
├── evidencias/                     # Destino de los logs/capturas de la corrida real (sección 9)
├── scripts/
│   ├── generar_datos.py            # Generador del dataset ampliado (reproducible, seed fija)
│   └── generar_evidencias.sh       # Automatiza la corrida local completa (Opción B, sección 9)
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
  vacío/no numérico/negativo.
- Calcula: `ahorro` → 1.5% mensual, `préstamo` → 2.5% mensual sobre el saldo, actualizando
  `saldo_final` mediante **upsert** (`ON CONFLICT (cuenta_id) DO UPDATE`).
- Los **duplicados lógicos** (mismo titular+saldo+edad+tipo bajo un número de cuenta
  distinto) se detectan igual que en `TransaccionItemProcessor`: a nivel de base de datos
  (restricción `UNIQUE uq_cuenta_interes_natural`), no en el processor. La primera versión
  los detectaba con un `Set` en memoria dentro del processor; una corrida real en GitHub
  Actions (22-08-2026) mostró que eso duplica falsos positivos bajo el "scanning" item por
  item que Spring Batch aplica tras un chunk fallido (ver el javadoc de la clase para el
  detalle) — se corrigió moviendo la detección a la base de datos, que sí participa de la
  transacción del chunk y por lo tanto se revierte junto con él.

**`MovimientoAnualItemProcessor`**
- Corrige el formato de fecha legacy y rellena la descripción vacía con un valor por defecto.
- Corrige automáticamente el signo del monto cuando es inconsistente con el tipo de
  movimiento (un depósito no puede ser negativo; un retiro/compra no puede ser positivo) y
  deja la anomalía registrada para auditoría.
- Omite: tipo de movimiento no reconocido, monto vacío/no numérico/en cero.

## 4. Escalado y procesamiento paralelo

### 4.1 Modo multi-hilo (Semanas 1-2, línea base, sin cambios)

Tal como exigen las instrucciones específicas, los tres Steps de carga usan:

- **Chunk de tamaño 5**: `batch.chunk-size=5` (`application.yml`), aplicado con
  `.chunk(propiedades.getChunkSize(), transactionManager)`.
- **3 hilos de ejecución paralela**: `ThreadPoolTaskExecutor` con `corePoolSize = maxPoolSize
  = 3` (`InfraestructuraBatchConfig`), asignado al Step con `.taskExecutor(...)`.
- El `FlatFileItemReader` (no thread-safe) se envuelve en `SynchronizedItemStreamReader`
  para permitir lectura concurrente segura sin perder el paralelismo real, que ocurre en el
  procesamiento y la escritura de cada chunk.

### 4.2 Modo particionado (Semana 3, nuevo, alternativo)

La instrucción específica 5 de la semana 3 exige *"decidir entre implementar un match
optimizado para multi-threads o utilizar particiones"*, decidiendo además los parámetros
de esa configuración. La decisión tomada aquí es **implementar ambas técnicas y compararlas
empíricamente** (sección 4.3), agregando el particionado como modo alternativo sobre el Job
de mayor volumen (**`transaccionesDiariasJob`**), sin modificar el Step multi-hilo ya
validado en la semana 2 (que se conserva intacto y sigue siendo el modo por defecto de ese
Job).

Diferencia conceptual entre ambas técnicas: en el modo multi-hilo, **un único** `Step`
ejecuta chunks en paralelo sobre **un único** `ItemReader` compartido (sincronizado); en el
modo particionado, el archivo se divide de antemano en `N` rangos disjuntos de filas y cada
rango se procesa en una ejecución de `Step` **completamente independiente** (su propio
`StepExecution`, su propio `ItemReader` abriendo el archivo por su cuenta) — las particiones
no compiten por un recurso compartido y una falla en una partición no bloquea a las demás.

Nuevo Job: **`transaccionesDiariasParticionadoJob`** (mismo flujo de negocio que
`transaccionesDiariasJob`: purga → carga → control de calidad → resumen/revisión), con estos
componentes nuevos (`config/TransaccionesParticionadoJobConfig.java`,
`partition/TransaccionesRangoPartitioner.java`):

- **`TransaccionesRangoPartitioner`** (`Partitioner`): cuenta las filas de datos del CSV y
  las reparte en `batch.grid-size` rangos `[start, end]` de tamaño similar.
- **`transaccionParticionReader`** (`@StepScope`): una instancia de `FlatFileItemReader` por
  cada partición, que salta directamente a su rango asignado con
  `setCurrentItemCount(start-1)` / `setMaxItemCount(end)` — la clase importante aquí es
  `AbstractItemCountingItemStreamItemReader`, que al abrir el reader y no encontrar todavía
  (en el `ExecutionContext` propio de esa partición) una clave de reinicio previa, usa el
  valor que le fijamos para saltar eficientemente (`jumpToItem`, lectura línea por línea) al
  inicio de su rango.
- **`workerCargaTransaccionesStep`** (Step "minion"): reutiliza el mismo `ItemProcessor`,
  `ItemWriter` y las mismas políticas de skip/retry ya probadas en la semana 2, pero SIN
  `taskExecutor` propio — el paralelismo lo aporta correr varias instancias de este Step al
  mismo tiempo, no hilos dentro de una sola instancia.
- **`TaskExecutorPartitionHandler`** (`PartitionHandler`): lanza las `batch.grid-size`
  ejecuciones del Step minion en paralelo sobre un `ThreadPoolTaskExecutor` dedicado,
  dimensionado según ese mismo parámetro.
- **`particionTransaccionesStep`** (Step "manager", un `PartitionStep`): orquesta el
  particionado y agrega los resultados de todas las particiones.

Uso:

```bash
java -jar target/banco-xyz-batch-1.0.0.jar transacciones-particionado
java -jar target/banco-xyz-batch-1.0.0.jar transacciones-particionado --batch.grid-size=6
```

No forma parte de la opción `todos` a propósito, para no alterar la evidencia ya validada
del modo multi-hilo; se ejecuta explícitamente por su propio nombre.

### 4.3 Benchmark comparativo: encontrando la configuración óptima

Para cumplir el criterio *"comparando diferentes parámetros para encontrar la configuración
óptima"*, se ejecutó el mismo dataset (`transacciones.csv`, 600 filas de datos) bajo
distintas configuraciones, usando en cada caso el `duracion={} ms` que
`JobResumenListener` imprime al finalizar el Job (tiempo total de punta a punta, incluyendo
purga, carga y resumen/revisión) como métrica de comparación:

| Parámetro variado | Job | Valores probados | Fijo |
|---|---|---|---|
| `batch.hilos` (multi-hilo) | `transaccionesDiariasJob` | 1, 3, 6 | `chunk-size=5` |
| `batch.chunk-size` (multi-hilo) | `transaccionesDiariasJob` | 1, 5, 20, 50 | `hilos=3` |
| `batch.grid-size` (particionado) | `transaccionesDiariasParticionadoJob` | 1, 2, 4, 6 | `chunk-size=5` |

La corrida real de este benchmark (vía GitHub Actions, ver
[`.github/workflows/evidencia-ejecucion-s3.yml`](.github/workflows/evidencia-ejecucion-s3.yml))
y sus resultados numéricos quedan documentados en
[`GUIA_EJECUCION.md`](GUIA_EJECUCION.md#benchmark-comparativo-semana-3) y en
`evidencias/benchmark-*.log`. La conclusión (con los números concretos de esa corrida) se
resume ahí mismo, junto con la configuración recomendada como "óptima" para este volumen de
datos y por qué.

> Nota de diseño: con un dataset de 600 filas, tanto el paralelismo por hilos como por
> particiones tienen overhead de arranque (creación de hilos, apertura de N streams de
> archivo) comparable o mayor al tiempo de procesamiento en sí — por eso el benchmark es
> justamente lo que permite decidir con evidencia, y no por intuición, si conviene escalar
> más allá de 3 hilos/particiones para este volumen, o si el punto óptimo real coincide con
> el mínimo exigido por las instrucciones.

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
- Todos los parámetros clave (`chunk-size`, `hilos`, `grid-size`, `limite-omisiones`,
  `maximo-reintentos`, `umbral-calidad-porcentaje`, rutas de los CSV) están externalizados en
  `application.yml` bajo el prefijo `batch`, y pueden ajustarse en caliente vía argumentos de
  línea de comandos sin recompilar — esto es lo que permite correr el benchmark comparativo
  de la sección 4.3 sin tocar código.
- **`particionTaskExecutor`** (solo para el modo particionado): dimensionado dinámicamente
  según `batch.grid-size` en vez de un valor fijo, precisamente porque ese es el parámetro
  que el benchmark hace variar.

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

## 8. Cómo subir este proyecto a tu cuenta de GitHub

Este proyecto ya quedó inicializado como repositorio Git local (ver `git log`) y conserva el
historial completo de las semanas 1-2. Al ser una **actividad individual**, se publica en un
repositorio propio nuevo (distinto del repositorio grupal de la semana 2) —paso previo
obligatorio para la Opción A de la sección siguiente—:

```bash
# 1. Crea un repositorio vacío en GitHub (sin README/licencia) llamado, por ejemplo,
#    "Backend3_Exp1_S3_Fernando_Fuentes_Allende", desde https://github.com/new

# 2. Desde la carpeta del proyecto:
git remote add origin https://github.com/<tu-usuario>/Backend3_Exp1_S3_Fernando_Fuentes_Allende.git
git branch -M main
git push -u origin main
```

## 9. Cómo obtener la evidencia de ejecución real

Hay dos caminos, no excluyentes, para obtener la "corrida real" del proyecto:

### Opción A — Automática, vía GitHub Actions (recomendada, no requiere instalar nada)

El proyecto incluye **dos** workflows independientes:

- [`.github/workflows/evidencia-ejecucion.yml`](.github/workflows/evidencia-ejecucion.yml):
  el de las semanas 1-2, sin cambios. Compila el proyecto, levanta un PostgreSQL real, ejecuta
  los 3 Jobs base (más la corrida de la rama `REVISION_REQUERIDA`) y publica los logs como
  artefacto `evidencia-ejecucion-banco-xyz`.
- [`.github/workflows/evidencia-ejecucion-s3.yml`](.github/workflows/evidencia-ejecucion-s3.yml):
  **nuevo de la semana 3**. Ejecuta el Job particionado y el benchmark comparativo completo
  de la sección 4.3 (variando hilos, chunk-size y grid-size), publicando los logs como
  artefacto `evidencia-ejecucion-s3-benchmark`.

1. Haz `git push` (paso anterior). Ambos workflows se disparan automáticamente.
2. En GitHub, entra a la pestaña **Actions** de tu repositorio y abre cada ejecución. Cada
   una tarda entre 2 y 6 minutos (el de la semana 3 corre más invocaciones del JAR, por el
   benchmark).
3. Si necesitas volver a lanzarlos sin hacer otro commit, usa el botón **Run workflow** de
   cada uno (habilitado gracias a `workflow_dispatch`).
4. Al terminar en verde, baja hasta la sección **Artifacts** de cada página y descarga el
   `.zip` correspondiente.
5. Descomprime ambos zips dentro de `evidencias/` y toma además una captura de pantalla de
   la pestaña Actions en verde — esa captura, sumada a los `.log`, es la evidencia que
   exigen las instrucciones.

Esta corrida es real: un runner de GitHub compila el JAR con Maven Central, levanta un
contenedor de PostgreSQL de verdad y ejecuta la aplicación exactamente igual que en tu propia
máquina — simplemente ocurre en la infraestructura de GitHub en vez de en un entorno local.

### Opción B — Local, en tu propio computador (si ya tienes Docker y Maven instalados)

Requiere JDK 21, Maven 3.9+ y Docker.

```bash
chmod +x scripts/generar_evidencias.sh
./scripts/generar_evidencias.sh

# Semana 3: Job particionado + benchmark comparativo
chmod +x scripts/benchmark_escalamiento.sh
./scripts/benchmark_escalamiento.sh
```

El primer script levanta PostgreSQL con `docker compose`, compila, ejecuta los 3 Jobs base
más la corrida de la rama alterna del decider, corre las consultas SQL de verificación, y
deja todo ordenado y con timestamp en `evidencias/corrida-<fecha-hora>/`. El segundo
(`benchmark_escalamiento.sh`) ejecuta el Job particionado y el benchmark comparativo de la
sección 4.3, dejando los resultados en `evidencias/benchmark-<fecha-hora>/`. Al final de cada
uno se imprime en pantalla qué buscar en cada log (los hilos `Batch-Thread-`/
`Particion-Thread-`, las métricas de `RendimientoStepListener`, la línea `FIN JOB` de
`JobResumenListener`, etc.).

Si prefieres ejecutar los Jobs manualmente uno por uno:

```bash
docker compose up -d
mvn clean package
java -jar target/banco-xyz-batch-1.0.0.jar transacciones
java -jar target/banco-xyz-batch-1.0.0.jar intereses
java -jar target/banco-xyz-batch-1.0.0.jar cuentas-anuales
# rama alterna del decider (dataset original, ~60% de registros invalidos en intereses.csv):
java -jar target/banco-xyz-batch-1.0.0.jar intereses --batch.rutas.intereses=classpath:sample-data/intereses.csv
# verificacion en base de datos:
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM resumen_transacciones_diarias;"
```

### Qué debe contener `evidencias/` al final

La carpeta [`evidencias/`](evidencias/) debe quedar con, como mínimo: el log completo de
cada uno de los 3 Jobs (desde `INICIO JOB` hasta `FIN JOB`, incluyendo el resumen de
`RendimientoStepListener`), el log de la corrida que muestra la rama `REVISION_REQUERIDA`, el
resultado de las consultas SQL de verificación, y —si usaste la Opción A— una captura de la
ejecución en verde en la pestaña Actions de GitHub.

## 10. Trazabilidad con la pauta de evaluación sumativa (Semana 3, 100 puntos)

| N° | Criterio de la pauta (resumen) | Puntos | Dónde se implementa |
|---|---|---|---|
| 1 | Diseña una arquitectura en Spring Batch según requerimientos del proyecto | 15 | 3 Jobs multi-Step (`config/*JobConfig.java`), estructura por capas (`model`, `processor`, `policy`, `listener`, `tasklet`, `partition`) — sección 2 |
| 2 | Ejecuta los Jobs y Steps configurados, procesando todos los datos y generando la salida esperada en la BD | 15 | Los 4 Jobs (3 base + particionado) leen 100% del CSV correspondiente y escriben en PostgreSQL (`schema.sql`); evidencia real en `evidencias/` (sección 9) |
| 3 | Implementa las transformaciones y validaciones de datos en el ItemProcessor | 15 | `TransaccionItemProcessor`, `InteresItemProcessor`, `MovimientoAnualItemProcessor` — sección 3 |
| 4 | Implementa técnicas de escalado (multi-threads o particiones) comparando parámetros para encontrar la configuración óptima | 15 | Multi-hilo (sección 4.1, semanas 1-2) **+** particionado real nuevo (sección 4.2) **+** benchmark comparativo de hilos/chunk-size/grid-size (sección 4.3, `GUIA_EJECUCION.md`) |
| 5 | Configura adecuadamente políticas de reintento y tolerancia a fallos, asegurando estabilidad y continuidad | 15 | `ConexionTransitoriaRetryPolicy`, `RegistroInvalidoSkipPolicy`, re-ejecución vía `JobRepository`/`runId` — sección 5 |
| 6 | Aplica las políticas y configuraciones de Spring Batch optimizando tiempos de ejecución y estabilidad del sistema | 15 | `BatchProperties` externalizado, HikariCP dimensionado, `ThreadPoolTaskExecutor`/`particionTaskExecutor` — sección 6 |
| 7 | Entrega los aspectos claves del caso (código fuente, documentación y evidencia de ejecución) | 10 | Repositorio individual en GitHub, este `README.md` + `GUIA_EJECUCION.md`, evidencia real en `evidencias/` vía GitHub Actions — secciones 8-9 |

## 11. Nota sobre el entorno en que se preparó este proyecto

Este proyecto fue generado y revisado en un entorno sandbox cuya política de red bloquea
explícitamente Maven Central (`repo.maven.apache.org` responde `403 Forbidden`) y Docker Hub
(`registry-1.docker.io` responde `403 Forbidden`), aunque el propio daemon de Docker sí pudo
iniciarse. En la práctica, eso significa que **no pudo compilarse ni ejecutarse dentro de ese
entorno**: por eso se agregó el workflow de GitHub Actions de la sección 9 (Opción A), que
corre en infraestructura con acceso normal a internet y es, hoy, la forma más directa de
obtener una corrida real sin depender de un equipo local. Cada firma de API utilizada (Spring
Batch 5.1.2 / Spring Retry, las versiones exactas que trae `spring-boot-starter-parent:3.3.4`)
fue verificada línea por línea contra el código fuente oficial de esas librerías antes de
escribirse aquí — incluyendo, para el particionado de la semana 3, `StepBuilder`,
`PartitionStepBuilder`, `TaskExecutorPartitionHandler` y, en particular,
`AbstractItemCountingItemStreamItemReader.open()` (para confirmar que `setCurrentItemCount`
efectivamente hace saltar al reader hasta el inicio del rango de la partición) — pero se
recomienda como primer paso revisar la salida de la Opción A antes de dar por definitiva la
entrega.
