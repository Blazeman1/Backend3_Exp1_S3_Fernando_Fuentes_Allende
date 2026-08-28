# Guía de ejecución — Banco XYZ Batch

Esta guía te lleva de "el código está listo" a "tengo la evidencia de ejecución en la mano".
Hay dos rutas; con seguir **una** de las dos basta, pero puedes hacer ambas.

- **Ruta A — GitHub Actions**: no instalas nada, todo corre en la nube de GitHub. Recomendada
  si no tienes Docker o Maven configurados localmente, o si prefieres evidencia reproducible
  que cualquier persona puede volver a generar abriendo el repositorio.
- **Ruta B — Local**: para quienes ya tienen Docker y Maven instalados y quieren ver la
  ejecución en su propia terminal.

## Antes de empezar

- El proyecto (`Exp1_S3_Fernando_Fuentes_Allende.zip`) descomprimido en tu computador.
- Una cuenta de GitHub propia (Ruta A) y/o Docker + JDK 21 + Maven 3.9+ instalados (Ruta B).
- 10-15 minutos.

---

## Ruta A — GitHub Actions (recomendada)

### 1. Sube el proyecto a tu cuenta de GitHub

Si todavía no lo hiciste:

```bash
cd Exp1_S3_Fernando_Fuentes_Allende
git remote add origin https://github.com/<tu-usuario>/banco-xyz-batch.git
git branch -M main
git push -u origin main
```

Si ya lo subiste antes y solo quieres disparar una corrida nueva, salta al paso 3.

### 2. Deja que el push dispare el workflow

En cuanto el `push` termina, GitHub ya está compilando el proyecto y levantando un
PostgreSQL real en segundo plano — no necesitas hacer nada más en tu terminal.

### 3. Observa la ejecución

1. Entra a tu repositorio en github.com.
2. Ve a la pestaña **Actions** (en la barra superior del repo).
3. Verás **dos** workflows disparados por el mismo push:
   - **"Evidencia de ejecución - Banco XYZ Batch"** (misma estructura de pasos heredada de las
     semanas 1-2, corriendo ahora contra el dataset oficial de la semana 3): 2-4 minutos.
   - **"Evidencia S3 - Particionado y Benchmark"** (nuevo): 4-6 minutos, porque además del
     Job particionado corre el benchmark comparativo (múltiples invocaciones del JAR variando
     hilos, chunk-size y grid-size).
4. Si quieres relanzar alguno sin hacer otro commit, usa el botón **Run workflow** de ese
   workflow (arriba a la derecha de su propia pantalla).

### 4. Descarga la evidencia

1. Cuando cada run termine (ícono verde ✓), baja hasta el final de esa página.
2. En la sección **Artifacts** de cada uno, descarga `evidencia-ejecucion-banco-xyz.zip` y
   `evidencia-ejecucion-s3-benchmark.zip` respectivamente.
3. Descomprime ambos: el primero trae 7 archivos `.log` (`evidencia-01-build.log` hasta
   `evidencia-07-consultas-sql.log`, incluyendo las dos corridas contra `sample-data/` que
   muestran la rama `CALIDAD_OK` en transacciones e intereses); el segundo trae los logs del
   Job particionado y del benchmark (`evidencia-01-build.log` hasta
   `evidencia-06-resumen-benchmark.log`).
4. Mueve todos esos archivos a la carpeta `evidencias/` del proyecto.
5. Toma una captura de pantalla de cada página de Actions en verde y guárdala también en
   `evidencias/`.

Eso es todo: ya tienes logs de consola reales más capturas de pantalla, exactamente lo que
piden las instrucciones específicas.

<a id="benchmark-comparativo-semana-3"></a>

### 5. Benchmark comparativo (Semana 3): cómo leer `evidencia-06-resumen-benchmark.log`

Ese log consolida, para cada corrida, la línea `FIN JOB [...] duracion=<N> ms` que imprime
`JobResumenListener` — es la métrica de comparación. Al abrirlo verás bloques como:

```
=== hilos=1 chunk=5 ===
...
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=1242 ms | ...
=== hilos=3 chunk=5 ===
...
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=1217 ms | ...
=== hilos=6 chunk=5 ===
...
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=1248 ms | ...
=== hilos=3 chunk=1 ===
...
=== gridSize=1 chunk=5 ===
...
FIN JOB [transaccionesDiariasParticionadoJob] - estado=COMPLETED | duracion=1190 ms | ...
=== gridSize=4 chunk=5 ===
...
```

(valores de una corrida real en GitHub Actions, 28-08-2026, sobre el dataset oficial completo
de 1000 filas; ver tabla completa mas abajo)

| Configuración | Job | Duración real | Throughput real |
|---|---|---|---|
| `hilos=1 chunk=5` | multi-hilo | 1242 ms | — |
| `hilos=3 chunk=5` (línea base exigida) | multi-hilo | 1217 ms | — |
| `hilos=6 chunk=5` | multi-hilo | 1248 ms | — |
| `hilos=3 chunk=1` | multi-hilo | 1836 ms | 429.9 items/seg |
| `hilos=3 chunk=5` | multi-hilo | 1217 ms | 664.6 items/seg |
| `hilos=3 chunk=20` | multi-hilo | 1350 ms | 592.7 items/seg |
| `hilos=3 chunk=50` | multi-hilo | 1703 ms | 464.6 items/seg |
| `gridSize=1 chunk=5` | particionado | 1190 ms | — |
| `gridSize=2 chunk=5` | particionado | 1023 ms | — |
| `gridSize=4 chunk=5` | particionado | 951 ms | — |
| `gridSize=6 chunk=5` | particionado | 862 ms | — |

Lectura de estos números reales (1000 filas, dataset oficial completo, 23.9% de omision -
sección 1.1 del README - aplica por igual a todas las filas de la tabla, así que no afecta
la comparación relativa entre configuraciones):

- **`hilos`**: la diferencia entre 1, 3 y 6 hilos es marginal (1242/1217/1248 ms) porque el
  volumen (1000 filas, 200 chunks de 5) es demasiado chico para que el paralelismo por hilos
  se note: el overhead de crear/coordinar hilos casi compensa exactamente la ganancia de
  paralelizar. `hilos=3` (la línea base exigida) resulta, de hecho, la más rápida de las tres,
  aunque por un margen dentro del ruido normal de una VM compartida de CI.
- **`chunk-size`**: acá sí hay una curva clara con forma de U, con óptimo en `chunk=5`
  (664.6 items/seg): `chunk=1` (429.9 items/seg) multiplica los commits (200 chunks pasan a
  1000, uno por fila) y con ellos el overhead transaccional; `chunk=50` (464.6 items/seg) va
  al otro extremo, con chunks tan grandes que un solo commit mueve 50 filas a la vez y reduce
  el paralelismo real percibido entre los 3 hilos.
- **`grid-size`** (particionado): a diferencia de lo que se esperaría en teoría (que el
  overhead de abrir N streams del mismo archivo terminara por pesar más que el trabajo a
  partir de cierto `grid-size`), la corrida real muestra una mejora **monótona** conforme
  crece `grid-size`: 1190 ms -> 1023 ms -> 951 ms -> 862 ms para 1, 2, 4 y 6 particiones. Para
  este volumen (1000 filas) el overhead de abrir más streams simplemente no llegó a pesar más
  que la ganancia de paralelismo real de leer/procesar/escribir cada partición de forma
  independiente; con un archivo mucho más grande, en algún punto esa curva debería aplanarse
  o revertirse, pero eso no se observó dentro del rango probado (1, 2, 4, 6).

Nota: en todas estas corridas el Job termina en `BatchStatus.COMPLETED` con `ExitStatus`
`REVISION_REQUERIDA` (no `FAILED`) — es la conducta esperada del `ControlCalidadDecider` frente
al dataset oficial completo, ver sección 5 del `README.md`; no indica un problema con la
configuración de escalado que se está comparando.

**Conclusión:** para este volumen de datos (1000 filas), la configuración más rápida de TODAS
las probadas fue el modo **particionado con `grid-size=6`** (862 ms), superando incluso al
mejor resultado del modo multi-hilo (`hilos=3 chunk=5`, 1217 ms). Dentro del modo multi-hilo
exigido por la pauta, la configuración óptima es `hilos=3 chunk=5` (la línea base): `hilos`
no aporta una mejora medible a este volumen, pero `chunk-size=5` sí es claramente superior a
valores mucho más chicos (overhead transaccional) o más grandes (menos paralelismo real). Para
un volumen de datos significativamente mayor (por ejemplo, miles o millones de filas, como
sería el caso real de transacciones diarias de un banco), se esperaría que el paralelismo por
hilos se note más, y que el particionado eventualmente encuentre el punto donde el overhead de
abrir N streams empiece a pesar más que la ganancia — este benchmark documenta el
comportamiento observado en el volumen de prueba disponible (1000 filas), no una regla
universal.

---

## Ruta B — Local, con Docker y Maven

### 1. Verifica que tienes lo necesario

```bash
docker --version
mvn --version
java --version   # debe ser 21
```

### 2. Ejecuta el script automático

```bash
cd Exp1_S3_Fernando_Fuentes_Allende
chmod +x scripts/generar_evidencias.sh
./scripts/generar_evidencias.sh
```

El script hace, en orden: levanta PostgreSQL con `docker compose`, espera a que esté
saludable, compila con Maven, corre los 3 Jobs contra el dataset oficial completo
(transacciones e intereses derivan a `REVISION_REQUERIDA` por su tasa de error real; cuentas
anuales completa el flujo normal), corre una cuarta y quinta vez transacciones e intereses
contra el subconjunto curado de `sample-data/` (para mostrar la rama `CALIDAD_OK` también en
esos dos Jobs), y ejecuta 4 consultas SQL de verificación. Todo queda guardado con timestamp
en `evidencias/corrida-<fecha-hora>/`.

Para el Job particionado y el benchmark comparativo de la semana 3 (con PostgreSQL ya
levantado y el proyecto ya compilado por el paso anterior):

```bash
chmod +x scripts/benchmark_escalamiento.sh
./scripts/benchmark_escalamiento.sh
```

Deja los resultados, con timestamp, en `evidencias/benchmark-<fecha-hora>/`.

### 3. Si prefieres ir comando por comando

```bash
docker compose up -d
mvn clean package

java -jar target/banco-xyz-batch-1.0.0.jar transacciones
java -jar target/banco-xyz-batch-1.0.0.jar intereses
java -jar target/banco-xyz-batch-1.0.0.jar cuentas-anuales
# transacciones e intereses terminan COMPLETED con ExitStatus REVISION_REQUERIDA (23.9% y
# 80.4% de omision real respectivamente, no es un FAILED); cuentas-anuales, CALIDAD_OK (~6%)

# rama CALIDAD_OK del decider, con el subconjunto curado (0% de omision, verificado):
java -jar target/banco-xyz-batch-1.0.0.jar transacciones \
  --batch.rutas.transacciones=classpath:sample-data/transacciones.csv
# intereses no tiene Step de purga propio (upsert-only): se vacia cuentas_interes antes de esta
# corrida para que no la contamine el estado (claves naturales) que dejo la corrida completa
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz \
  -c "TRUNCATE TABLE cuentas_interes;"
java -jar target/banco-xyz-batch-1.0.0.jar intereses \
  --batch.rutas.intereses=classpath:sample-data/intereses.csv

# Semana 3: Job particionado (modo alternativo del Job de transacciones)
java -jar target/banco-xyz-batch-1.0.0.jar transacciones-particionado
java -jar target/banco-xyz-batch-1.0.0.jar transacciones-particionado --batch.grid-size=6

# verificacion en base de datos:
docker exec -it banco-xyz-postgres psql -U banco_xyz -d banco_xyz \
  -c "SELECT * FROM resumen_transacciones_diarias;"
```

### 4. Captura la evidencia

Guarda la salida de cada comando (copia el texto de la terminal, o usa `| tee archivo.log`)
y toma una captura de pantalla de al menos una corrida completa mostrando los hilos
`Batch-Thread-1/2/3` trabajando en paralelo.

---

## Qué debe quedar en `evidencias/` al final

| Archivo / captura | Qué demuestra |
|---|---|
| Log del build (`mvn clean package` exitoso) | El proyecto compila |
| Log de `transaccionesDiariasJob` (dataset oficial) | Chunks + 3 hilos + rama `REVISION_REQUERIDA` (23.9% de omisión real: 215 al procesar + 24 por duplicado al escribir); Job en `COMPLETED` / `ExitStatus` `REVISION_REQUERIDA`, no `FAILED` |
| Log de `interesesMensualesJob` (dataset oficial) | Cálculo de intereses + upsert + rama `REVISION_REQUERIDA` (80.4% de omisión real); Job en `COMPLETED` / `ExitStatus` `REVISION_REQUERIDA`, no `FAILED` |
| Log de `estadosCuentaAnualesJob` (dataset oficial) | Agregación anual por cuenta + rama `CALIDAD_OK` (6% de omisión real) |
| Logs de las corridas con `sample-data/transacciones.csv` y `sample-data/intereses.csv` | Rama `CALIDAD_OK` del decider, también para esos dos Jobs |
| Log o captura de las consultas SQL | Los datos realmente quedaron en PostgreSQL |
| Captura de la pestaña Actions en verde (Ruta A, ambos workflows) | Evidencia reproducible, visible para cualquiera |
| Log de `transaccionesDiariasParticionadoJob` | Particionado real (`Particion-Thread-`) funcionando |
| Logs del benchmark (`benchmark-hilos`, `benchmark-chunk`, `benchmark-gridsize`) + resumen consolidado | Comparación de parámetros para encontrar la configuración óptima (criterio 4 de la pauta S3) |

## Qué buscar dentro de cada log (para verificar que de verdad funcionó)

- **`[Batch-Thread-1]`, `[Batch-Thread-2]`, `[Batch-Thread-3]`** intercalados en el log: eso
  es el paralelismo real ocurriendo (criterio "escala el procesamiento").
- Líneas `WARN ... Registro omitido` del `RegistroInvalidoSkipPolicy`: el manejo de errores
  actuando (criterio "maneja errores y excepciones").
- La línea `<== Fin Step [...] ... throughput=... items/seg` de `RendimientoStepListener`:
  las métricas de rendimiento (criterio "logs para evaluar el rendimiento").
- En las corridas contra el dataset oficial (transacciones e intereses), la línea
  `Porcentaje de omision (...) supera el umbral` seguida de `REVISION_REQUERIDA`; en las
  corridas contra `sample-data/` (subconjunto curado) y en `estadosCuentaAnualesJob`, la
  misma línea pero con `CALIDAD_OK`: el `JobExecutionDecider` mostrando ambas ramas con datos
  reales (criterio "políticas de finalización y re-ejecución").
- En la línea final `FIN JOB [...] - estado=<X> | ...`, cuando la rama fue `REVISION_REQUERIDA`
  el `estado` correcto es **`COMPLETED`** (no `FAILED`): la revisión requerida es una detención
  intencional antes del reporte final, no una falla del framework — ver la nota sobre el fix de
  `FlowBuilder.addDanglingEndStates()` en la sección 5 del `README.md` si ves `FAILED` en una
  copia desactualizada del proyecto.
- **`[Particion-Thread-0]`, `[Particion-Thread-1]`, ...** intercalados en el log del Job
  particionado: varias particiones procesando rangos disjuntos del archivo al mismo tiempo
  (criterio 4 de la pauta S3, técnica de escalado alternativa).
- La línea `Particion[N] asignada: filas X a Y` de `TransaccionesRangoPartitioner`: confirma
  cómo se repartió el archivo entre las particiones.
- Las líneas `FIN JOB [...] duracion=<N> ms` repetidas en los logs de benchmark, una por cada
  configuración probada: es la evidencia numérica de la comparación de parámetros.

## Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| `mvn clean package` falla por dependencias | Sin conexión a Maven Central | Revisa tu red; en la Ruta A esto no ocurre porque el runner de GitHub sí tiene acceso |
| `Connection refused` a PostgreSQL | El contenedor aún no está listo | Espera unos segundos más, o revisa `docker ps` y `docker logs banco-xyz-postgres` |
| Puerto 5432 ya en uso | Ya tienes un PostgreSQL corriendo localmente | Detén ese servicio o cambia el mapeo de puertos en `docker-compose.yml` |
| El workflow no aparece en Actions | El repo no tiene Actions habilitado, o el push fue a una rama distinta de `main`/`master` | Revisa Settings → Actions esté habilitado, y que el push haya sido a `main` |
| El Job 1 termina `COMPLETED` en el log pero el step de Actions se queda "corriendo" para siempre y nunca llega al Job 2 | La JVM no cierra sola: el `ThreadPoolTaskExecutor` (3 hilos, `corePoolSize == maxPoolSize`) crea hilos no-daemon que quedan vivos esperando tareas aunque el Job ya haya terminado | Ya corregido en `BancoXyzBatchApplication`: `main()` ahora cierra el contexto y fuerza la salida con `System.exit(SpringApplication.exit(contexto, ...))`. Si ves este síntoma, verifica que tu copia tenga ese cambio (commit "Corregir cierre de la JVM...") |
| Al correr `transacciones-particionado` con `--batch.grid-size` alto (por ejemplo 10+) aparecen errores de conexión a la base de datos | Cada partición concurrente reserva su propia conexión JDBC; si `grid-size` supera el `maximum-pool-size` de HikariCP (12 por defecto) las particiones sobrantes esperan o fallan | Para `grid-size` mayores a ~8-10, sube también `spring.datasource.hikari.maximum-pool-size` en `application.yml`, o pásalo por línea de comandos (`--spring.datasource.hikari.maximum-pool-size=20`) |
