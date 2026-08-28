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

> **Nota sobre continuidad:** la arquitectura de los 3 Jobs (Steps, listeners, políticas de
> skip/retry, decider de calidad) proviene de las actividades formativas previas y se
> mantiene sin cambios de diseño. Lo nuevo de la semana 3 es el modo de particionado y el
> benchmark comparativo (sección 4), **y el cambio de dataset de origen**: esta actividad usa
> el dataset OFICIAL de `data/semana_3` (no el de `semana_2`, mucho más pequeño y con menos
> variedad de errores), lo que además obligó a corregir/ampliar dos reglas de negocio que
> asumían un dataset más simple (parser de fechas y normalización de tipos de movimiento —
> ver sección 1.1 y 3).

Se implementan tres Jobs independientes, cada uno versionado como parte del mismo
proyecto Maven:

| Job | Descripción | Fuente | Destino |
|---|---|---|---|
| **`transaccionesDiariasJob`** | Reporte de Transacciones Diarias: procesa transacciones para detectar anomalías y generar un resumen. | `data/transacciones.csv` | `transacciones_procesadas`, `resumen_transacciones_diarias` |
| **`interesesMensualesJob`** | Cálculo de Intereses Mensuales: aplica intereses sobre cuentas de ahorro y préstamo, y actualiza el saldo final. | `data/intereses.csv` | `cuentas_interes` |
| **`estadosCuentaAnualesJob`** | Generación de Estados de Cuenta Anuales: compila el historial anual por cuenta en un informe para auditorías. | `data/cuentas_anuales.csv` | `cuentas_anuales_movimientos`, `estados_cuenta_anuales` |

### 1.1 Datos de origen (dataset oficial de la semana 3)

Los datos de origen son el dataset **oficial** de esta actividad:
[`KariVillagran/bank_legacy_data`](https://github.com/KariVillagran/bank_legacy_data),
carpeta [`data/semana_3`](https://github.com/KariVillagran/bank_legacy_data/tree/main/data/semana_3)
— **no** el de `data/semana_2` que se usó en las actividades formativas previas. A diferencia
de aquel (8-10 filas por archivo), este trae **1000 filas por archivo**, ya suficientes por sí
solas para evidenciar de forma convincente chunks, multithreading y particionado, sin
necesidad de generar datos sintéticos adicionales.

Los 3 CSV oficiales, **sin modificar**, se copiaron tal cual a `src/main/resources/data/` (la
ruta que usan los Jobs por defecto). Se verificó programáticamente (no a simple vista) qué
problemas de calidad trae realmente este dataset, porque difieren en variedad de los
descritos para `semana_2`:

| Archivo | Problemas de datos verificados (sobre 1000 filas) |
|---|---|
| `transacciones.csv` | Fechas en **4 formatos** distintos: `yyyy-MM-dd` (294), `yyyy/MM/dd` (222), `dd-MM-yyyy` (250) y `dd/MM/yyyy` (234); 55 fechas estructuralmente válidas pero con mes inexistente (p. ej. `2024-13-01`); montos vacíos (168) o no numéricos; montos negativos/cero (154, se conservan como anomalía); tipos `desconocido`/`invalid` además de `credito`/`debito` (294, se conservan como anomalía) |
| `intereses.csv` | Tipos de cuenta `ahorro` (219), `prestamo` (226) -los dos que exige la actividad-, más `hipoteca` (224, fuera del alcance del cálculo de intereses de esta actividad), `-1` (279) y `unknown` (52) como valores centinela de dato inválido; edades vacías o fuera de rango \[18-90\] (171); saldos vacíos o negativos (55); de los registros que pasan las tres validaciones anteriores, 23 se omiten igualmente en la escritura por `DuplicateKeyException` sobre la clave natural (nombre, saldo, edad, tipo) — ver la nota sobre reutilización de `cuenta_id` y upsert más abajo |
| `cuentas_anuales.csv` | Fechas en los mismos 4 formatos; tipo de movimiento `compra`/`retiro`/`deposito`, más `pago` (42, tratado igual que `retiro`/`compra`: dinero que sale de la cuenta) y **`depósito` con tilde** (52 filas — la ortografía correcta en español; se normaliza el acento en vez de descartarla); montos en cero o no numéricos (60); 485-523 filas con signo de monto inconsistente con el tipo de movimiento (se corrige automáticamente, ver sección 3) |

Esto llevó a corregir dos componentes que asumían un dataset más simple (el de `semana_2`,
con solo 2 formatos de fecha y sin la variante acentuada): `FechaFlexibleParser` (ahora
soporta los 4 formatos reales) y `MovimientoAnualItemProcessor` (ahora acepta `pago` y
normaliza acentos antes de comparar el tipo de movimiento) — ver sección 3.

> **Nota sobre `intereses.csv` — reutilización de `cuenta_id` e interacción con el upsert:**
> se verificó programáticamente que las 1000 filas de este archivo solo usan **50 valores
> distintos** de `cuenta_id` (cada uno se repite, en promedio, 20 veces). Como
> `InteresItemProcessor` escribe con **upsert** (`INSERT ... ON CONFLICT (cuenta_id) DO
> UPDATE`, porque `cuenta_id` es la clave primaria real de `cuentas_interes`), una fila
> posterior con el mismo `cuenta_id` no genera un registro nuevo: sobrescribe al anterior. Eso
> tiene una consecuencia no obvia sobre la detección de duplicados por clave natural
> (`nombre`, `saldo`, `edad`, `tipo`, ver tabla arriba y sección 3): si dos filas con
> `cuenta_id` distintos comparten esa clave natural, cuál de las dos termina realmente en
> conflicto (`DuplicateKeyException`) depende de si, para cuando llega la segunda, la primera
> sigue en la tabla o ya fue sobrescrita (upsert) por una tercera fila que compartía su
> `cuenta_id`. Una predicción ingenua que trata cada fila válida como un `INSERT`
> independiente estima 55 omisiones por duplicado; una simulación que sí modela el orden real
> de llegada y el efecto del upsert por `cuenta_id` predice 25; la evidencia real de CI mide
> **23** (la diferencia de 2 frente a la simulación es atribuible al orden de intercalado real
> entre los 3 hilos de ejecución paralela, no a un error de modelamiento). Es una
> característica genuina del dataset oficial, no un defecto del código.

Adicionalmente, en `src/main/resources/sample-data/` se dejaron **subconjuntos curados**
(filtrados programáticamente desde el propio dataset oficial, sin inventar ni modificar
ningún valor) que no contienen ninguna fila que dispare un `skip`, usados para demostrar
puntualmente la rama `CALIDAD_OK` del `ControlCalidadDecider` en Jobs cuyo dataset oficial
completo cae en la rama `REVISION_REQUERIDA` (ver sección 8). Ambas rutas son
intercambiables por configuración (`--batch.rutas.<job>=classpath:sample-data/<archivo>.csv`).
Cada subconjunto se filtró con la regla de "cero omisiones" que de verdad aplica a su Job:

- `sample-data/transacciones.csv` (**761 filas**): sin fechas/montos inválidos y, además,
  **deduplicado** sobre la misma clave natural (`fecha`, `monto`, `tipo`) que usa la
  restricción `UNIQUE` de la tabla — una primera versión de este archivo (785 filas) filtraba
  solo fecha/monto pero no deduplicaba, y 24 de esas filas seguían disparando
  `DuplicateKeyException` en la corrida real; la versión actual se verificó sin duplicados
  internos.
- `sample-data/intereses.csv` (**50 filas**): dado el upsert por `cuenta_id` explicado arriba,
  un archivo realmente "cero omisiones" para este Job no puede tener más de **una fila por
  `cuenta_id` distinto** (de lo contrario la segunda simplemente sobrescribe a la primera, sin
  aportar una fila nueva de verdad) y, entre esas filas de `cuenta_id` distintos, ninguna puede
  compartir su clave natural con otra. Una primera versión (164 filas) repetía varios
  `cuenta_id`, lo que causaba 5 omisiones reales por duplicado en la corrida de CI; la versión
  actual tiene exactamente una fila por cada uno de los 50 `cuenta_id` distintos del dataset
  oficial, sin colisiones de clave natural entre ellas, y se verificó con 0% de omisión real.

> `scripts/generar_datos.py` (generador de datos sintéticos usado en las semanas 1-2, cuando
> el dataset oficial de esa época solo traía 8-10 filas) queda como referencia histórica y ya
> no forma parte del flujo por defecto: el dataset oficial de `semana_3` no lo necesita.

## 2. Arquitectura del proyecto

```
banco-xyz-batch/
├── .github/workflows/
│   ├── evidencia-ejecucion.yml     # CI: compila y corre los 3 Jobs (dataset oficial + subconjunto curado)
│   └── evidencia-ejecucion-s3.yml  # CI: Job particionado + benchmark comparativo (semana 3)
├── docker-compose.yml              # PostgreSQL para desarrollo local
├── pom.xml
├── evidencias/                     # Destino de los logs/capturas de la corrida real (sección 8)
├── scripts/
│   ├── generar_datos.py            # (Historico S1-S2) generador del dataset sintetico ampliado
│   └── generar_evidencias.sh       # Automatiza la corrida local completa (Opción B, sección 8)
├── src/main/resources/
│   ├── application.yml
│   ├── schema.sql                  # Tablas de negocio (metadata de Spring Batch se autogenera)
│   ├── data/                       # CSV oficiales de data/semana_3 (usados por defecto)
│   └── sample-data/                # Subconjuntos curados (sin filas invalidas) del mismo dataset oficial
└── src/main/java/com/bancoxyz/batch/
    ├── BancoXyzBatchApplication.java   # Punto de entrada + selector de Job por CLI
    ├── config/                          # Configuración de los 3 Jobs + infraestructura común
    │   ├── BatchProperties.java             # chunk-size, hilos, límites, rutas (externalizados)
    │   ├── InfraestructuraBatchConfig.java   # TaskExecutor (3 hilos) y JdbcTemplate
    │   ├── TransaccionesJobConfig.java
    │   ├── TransaccionesParticionadoJobConfig.java  # Job particionado (semana 3, sección 4.2)
    │   ├── InteresesJobConfig.java
    │   └── CuentasAnualesJobConfig.java
    ├── model/                            # POJOs "raw" (CSV) y "procesados" (BD)
    ├── processor/                        # ItemProcessor: valida, corrige u omite
    ├── policy/                           # SkipPolicy y RetryPolicy personalizadas
    ├── listener/                         # Listeners de Job/Step/Skip + JobExecutionDecider
    ├── partition/                        # TransaccionesRangoPartitioner (semana 3, sección 4.2)
    ├── tasklet/                          # Tasklets: purga, resumen, agregación, revisión
    ├── exception/                        # InvalidDataException
    └── util/                             # FechaFlexibleParser (4 formatos: ver seccion 1.1)
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
- Corrige el formato de fecha: `FechaFlexibleParser` prueba, en orden, `yyyy-MM-dd`,
  `yyyy/MM/dd`, `dd-MM-yyyy` y `dd/MM/yyyy` — los 4 formatos verificados en el dataset oficial
  de la semana 3 (ver sección 1.1); verificado además con un caso de prueba en Java puro
  (JDK 21) que confirma que no hay ambigüedad entre formatos (p. ej. `01-01-2024` nunca se
  malinterpreta como año-mes-día).
- Conserva y marca como anomalía: monto negativo, monto en cero, tipo no reconocido
  (`desconocido`, `invalid`, o cualquier valor fuera de `DEBITO`/`CREDITO`).
- Omite (skip): monto vacío o no numérico, fecha imposible de interpretar (estructuralmente
  válida pero con mes/día inexistente, p. ej. `2024-13-01`, presente en el dataset real).
- Los **duplicados exactos** (misma fecha+monto+tipo) se detectan a nivel de base de datos
  (restricción `UNIQUE`) y se omiten en la escritura — ver `RegistroInvalidoSkipPolicy`.

**`InteresItemProcessor`**
- Omite: tipo de cuenta fuera de alcance (`hipoteca`, `-1`, `unknown`, vacío — el
  requerimiento solo contempla `ahorro` y `préstamo`; en el dataset oficial esto por sí solo
  representa el 55.5% de las filas, ver sección 1.1), edad vacía/no numérica/fuera de rango
  \[18-90\], saldo vacío/no numérico/negativo.
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
- Corrige el formato de fecha (los mismos 4 formatos que `TransaccionItemProcessor`) y
  rellena la descripción vacía con un valor por defecto.
- **Corrige** (no descarta) el tipo de movimiento `depósito` con tilde, normalizando el
  acento antes de compararlo contra `deposito` — verificado con un caso de prueba en Java
  puro que confirma que `Normalizer.normalize(..., NFD)` + eliminar marcas de combinación
  hace que ambas cadenas se comparen como iguales. Sin esta corrección, el 5.2% de las filas
  del dataset oficial (la ortografía correcta en español) se habría descartado por error.
- Acepta `pago` como tipo de movimiento válido, tratado igual que `retiro`/`compra` (dinero
  que sale de la cuenta) — el dataset oficial lo usa en el 4.2% de las filas y no hay
  ninguna razón de negocio para excluirlo.
- Corrige automáticamente el signo del monto cuando es inconsistente con el tipo de
  movimiento (un depósito no puede ser negativo; un retiro/compra/pago no puede ser positivo)
  y deja la anomalía registrada para auditoría.
- Omite: tipo de movimiento no reconocido (tras normalizar el acento), monto vacío/no
  numérico/en cero.

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

> **Nota de evidencia real (28-08-2026):** el primer benchmark real de este Job particionado
> reveló dos bugs adicionales, ambos ya corregidos:
>
> 1. `transaccionesDiariasParticionadoJob` reutiliza `revisionTransaccionesStep` (el mismo Step
>    que usa `transaccionesDiariasJob`) y tenía exactamente el mismo bug de `BatchStatus.FAILED`
>    descrito en la sección 5 más abajo -faltaba la transición explícita
>    `.on("*").end(...)` tras `.to(revisionTransaccionesStep)`- por lo que terminaba `FAILED` en
>    vez de `COMPLETED` cada vez que el `grid-size` probado hacía que el Job derivara a
>    `REVISION_REQUERIDA` (lo cual, con el dataset oficial completo, es siempre: 23.9% de
>    omisión sección 1.1). Se corrigió igual que en los otros dos Jobs.
> 2. `JobResumenListener` sumaba **el doble** de lo correcto (`leidos=2000`, `escritos=1522`,
>    `omitidos=478` en vez de 1000/761/239) al ejecutar el Job particionado. Causa: Spring Batch
>    agrega (copia) los contadores de las `grid-size` particiones dentro del propio
>    `StepExecution` del Step "manager" (`particionTransaccionesStep`) -confirmado leyendo el
>    log de `ControlCalidadDecider`, que ya mostraba el 23.9% correcto tomándolo de ese mismo
>    `StepExecution`- pero además cada partición hija queda registrada por separado en
>    `jobExecution.getStepExecutions()` con el nombre `workerCargaTransaccionesStep:particionN`.
>    Sumar TODAS las `StepExecution` (como hacía `JobResumenListener`) contaba dos veces lo
>    mismo: una vez ya agregado en el manager y otra vez por cada partición individual. Se
>    corrigió filtrando en `JobResumenListener` cualquier `StepExecution` cuyo nombre contenga
>    `":"` (la convención de nombres que Spring Batch usa exclusivamente para particiones hijas;
>    ningún Step normal de este proyecto lleva `":"` en su nombre, así que el filtro no afecta a
>    los demás Jobs).

### 4.3 Benchmark comparativo: encontrando la configuración óptima

Para cumplir el criterio *"comparando diferentes parámetros para encontrar la configuración
óptima"*, se ejecutó el mismo dataset (`transacciones.csv`, 1000 filas de datos (dataset oficial semana_3)) bajo
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

> Nota de diseño: con un dataset de 1000 filas, tanto el paralelismo por hilos como por
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
  (`batch.limite-omisiones`, **900** por defecto). Este límite es un circuito de seguridad
  contra pérdida silenciosa de datos, no el criterio de calidad en sí (ese lo evalúa el
  `ControlCalidadDecider`, por porcentaje) — se fijó en 900 tras verificar programáticamente
  la tasa de omisión real del peor caso del dataset oficial (`intereses.csv`, 80.4%, ver
  sección 1.1): con el valor de 200 usado en la semana 2 (pensado para un dataset de 300-900
  filas), el Step habría fallado abruptamente antes de terminar de leer el archivo.
- **`ConexionTransitoriaRetryPolicy`** (`RetryPolicy` personalizada, extiende
  `SimpleRetryPolicy`): reintenta únicamente fallas transitorias de infraestructura
  (`TransientDataAccessException`, `SQLTransientException`, `QueryTimeoutException`), hasta
  `batch.maximo-reintentos` intentos, dejando cada intento registrado en el log.
- **`ControlCalidadDecider`** (`JobExecutionDecider`): política de finalización que deriva el
  flujo del Job según el porcentaje de registros omitidos (`batch.umbral-calidad-porcentaje`,
  20% por defecto). Contra el dataset oficial de la semana 3 esto se demuestra con datos
  reales, sin necesidad de un dataset alterno: `transaccionesDiariasJob` (23.9% de omisión: 215
  omitidas al procesar por fecha/monto inválidos + 24 omitidas al escribir por
  `DuplicateKeyException` sobre la clave natural fecha+monto+tipo — duplicados reales presentes
  en el dataset oficial) e `interesesMensualesJob` (80.4%, ver desglose y la nota sobre el
  upsert por `cuenta_id` en la sección 1.1) derivan a `REVISION_REQUERIDA`, mientras que
  `estadosCuentaAnualesJob` (6.0%) completa el flujo normal (`CALIDAD_OK`) — y, para los dos
  primeros, `sample-data/` provee además un subconjunto curado que sí llega a `CALIDAD_OK`,
  demostrando ambas ramas para todos los Jobs (ver sección 9).
  > **Nota importante — `REVISION_REQUERIDA` no es un fallo del Job:** cuando el flujo deriva a
  > `REVISION_REQUERIDA`, el `RevisionRequeridaTasklet` deja constancia en el log y el Job
  > **termina en `BatchStatus.COMPLETED`** (no falla), con `ExitStatus` informativo
  > `REVISION_REQUERIDA`, simplemente deteniéndose antes de generar el reporte/resumen final —
  > ese fue siempre el diseño previsto. Esto requirió una corrección concreta: en la DSL de
  > flujo de Spring Batch, un Step sin transición saliente explícita ("colgante") recibe
  > automáticamente `on("COMPLETED").end()` **más** un `on("*").fail()` de respaldo (ver
  > `FlowBuilder.addDanglingEndStates()` en el código fuente del framework). Como el Step de
  > `RevisionRequeridaTasklet` termina con el `ExitStatus` personalizado `"REVISION_REQUERIDA"`
  > y no `"COMPLETED"`, caía en ese `"*"` de respaldo y el Job completo quedaba en
  > `BatchStatus.FAILED` — visible como `estado=FAILED` en la evidencia real de CI, pese a no
  > ser una falla real. `TransaccionesJobConfig` e `InteresesJobConfig` agregan ahora
  > explícitamente `.on("*").end(ControlCalidadDecider.REVISION_REQUERIDA)` justo después de
  > `.to(revisionXStep)`, dándole al Step una transición propia: el Job pasa a terminar en
  > `BatchStatus.COMPLETED` con `ExitStatus` `REVISION_REQUERIDA`, tal como se documenta en el
  > resto de esta guía.
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

- **Pool de conexiones (HikariCP)**: `maximum-pool-size: 12`, dimensionado para cubrir los 3
  hilos de procesamiento paralelo más margen para el `JobRepository`, los Tasklets de
  resumen y las particiones concurrentes del Job particionado (sección 4.2), sin
  sobredimensionar y agotar conexiones del servidor.
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

## 8. Cómo obtener la evidencia de ejecución real

Hay dos caminos, no excluyentes, para obtener la "corrida real" del proyecto:

### Opción A — Automática, vía GitHub Actions (recomendada, no requiere instalar nada)

El proyecto incluye **dos** workflows independientes:

- [`.github/workflows/evidencia-ejecucion.yml`](.github/workflows/evidencia-ejecucion.yml):
  heredado de las semanas 1-2 (misma estructura de pasos), con las corridas actualizadas para
  el dataset oficial de la semana 3. Compila el proyecto, levanta un PostgreSQL real, ejecuta
  los 3 Jobs base contra el dataset oficial completo (transacciones e intereses derivan a
  `REVISION_REQUERIDA` — Job termina `COMPLETED` con `ExitStatus` `REVISION_REQUERIDA`, no
  `FAILED`, ver sección 5 —; cuentas-anuales completa el flujo normal), más dos corridas
  adicionales de transacciones e intereses contra el subconjunto curado de `sample-data/`
  para mostrar la rama `CALIDAD_OK` también en esos dos Jobs (entre ambas corridas de
  intereses, el workflow vacía la tabla `cuentas_interes` con `TRUNCATE` vía `psql`: ese Job no
  tiene Step de purga propio —es upsert-only, ver sección 1.1— así que sin este paso la corrida
  contra `sample-data/` heredaría claves naturales de la corrida completa anterior), y publica
  los 7 logs como artefacto `evidencia-ejecucion-banco-xyz`.
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
# dataset oficial semana_3 completo: transacciones e intereses derivan a REVISION_REQUERIDA
# (23.9% y 80.4% de omision respectivamente; el Job termina COMPLETED con ExitStatus
# REVISION_REQUERIDA, no FAILED); cuentas-anuales completa el flujo (CALIDAD_OK, 6%)
java -jar target/banco-xyz-batch-1.0.0.jar transacciones
java -jar target/banco-xyz-batch-1.0.0.jar intereses
java -jar target/banco-xyz-batch-1.0.0.jar cuentas-anuales
# rama CALIDAD_OK del decider, con el subconjunto curado (0% de omision, verificado):
java -jar target/banco-xyz-batch-1.0.0.jar transacciones --batch.rutas.transacciones=classpath:sample-data/transacciones.csv
# intereses no tiene Step de purga propio (es upsert-only): se vacia cuentas_interes antes de
# esta corrida para que no quede contaminada por claves naturales de la corrida completa anterior
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "TRUNCATE TABLE cuentas_interes;"
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

## 9. Trazabilidad con la pauta de evaluación sumativa (Semana 3, 100 puntos)

| N° | Criterio de la pauta (resumen) | Puntos | Dónde se implementa |
|---|---|---|---|
| 1 | Diseña una arquitectura en Spring Batch según requerimientos del proyecto | 15 | 3 Jobs multi-Step (`config/*JobConfig.java`), estructura por capas (`model`, `processor`, `policy`, `listener`, `tasklet`, `partition`) — sección 2 |
| 2 | Ejecuta los Jobs y Steps configurados, procesando todos los datos y generando la salida esperada en la BD | 15 | Los 4 Jobs (3 base + particionado) leen 100% del CSV correspondiente y escriben en PostgreSQL (`schema.sql`); evidencia real en `evidencias/` (sección 9) |
| 3 | Implementa las transformaciones y validaciones de datos en el ItemProcessor | 15 | `TransaccionItemProcessor`, `InteresItemProcessor`, `MovimientoAnualItemProcessor` — sección 3 |
| 4 | Implementa técnicas de escalado (multi-threads o particiones) comparando parámetros para encontrar la configuración óptima | 15 | Multi-hilo (sección 4.1, semanas 1-2) **+** particionado real nuevo (sección 4.2) **+** benchmark comparativo de hilos/chunk-size/grid-size (sección 4.3, `GUIA_EJECUCION.md`) |
| 5 | Configura adecuadamente políticas de reintento y tolerancia a fallos, asegurando estabilidad y continuidad | 15 | `ConexionTransitoriaRetryPolicy`, `RegistroInvalidoSkipPolicy`, re-ejecución vía `JobRepository`/`runId` — sección 5 |
| 6 | Aplica las políticas y configuraciones de Spring Batch optimizando tiempos de ejecución y estabilidad del sistema | 15 | `BatchProperties` externalizado, HikariCP dimensionado, `ThreadPoolTaskExecutor`/`particionTaskExecutor` — sección 6 |
| 7 | Entrega los aspectos claves del caso (código fuente, documentación y evidencia de ejecución) | 10 | Repositorio individual en GitHub, este `README.md` + `GUIA_EJECUCION.md`, evidencia real en `evidencias/` vía GitHub Actions — secciones 8-9 |
