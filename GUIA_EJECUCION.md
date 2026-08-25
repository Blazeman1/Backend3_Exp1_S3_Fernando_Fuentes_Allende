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
   - **"Evidencia de ejecución - Banco XYZ Batch"** (semanas 1-2, sin cambios): 2-3 minutos.
   - **"Evidencia S3 - Particionado y Benchmark"** (nuevo): 4-6 minutos, porque además del
     Job particionado corre el benchmark comparativo (múltiples invocaciones del JAR variando
     hilos, chunk-size y grid-size).
4. Si quieres relanzar alguno sin hacer otro commit, usa el botón **Run workflow** de ese
   workflow (arriba a la derecha de su propia pantalla).

### 4. Descarga la evidencia

1. Cuando cada run termine (ícono verde ✓), baja hasta el final de esa página.
2. En la sección **Artifacts** de cada uno, descarga `evidencia-ejecucion-banco-xyz.zip` y
   `evidencia-ejecucion-s3-benchmark.zip` respectivamente.
3. Descomprime ambos: el primero trae 6 archivos `.log` (`evidencia-01-build.log` hasta
   `evidencia-06-consultas-sql.log`); el segundo trae los logs del Job particionado y del
   benchmark (`evidencia-01-build.log` hasta `evidencia-06-resumen-benchmark.log`).
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
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=<N1> ms | ...
=== hilos=3 chunk=5 ===
...
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=<N2> ms | ...
=== hilos=6 chunk=5 ===
...
FIN JOB [transaccionesDiariasJob] - estado=COMPLETED | duracion=<N3> ms | ...
=== hilos=3 chunk=1 ===
...
=== gridSize=1 chunk=5 ===
...
FIN JOB [transaccionesDiariasParticionadoJob] - estado=COMPLETED | duracion=<N4> ms | ...
=== gridSize=4 chunk=5 ===
...
```

Para completar la conclusión con datos reales: copia aquí, en esta misma sección, la tabla de
duraciones obtenida en TU corrida real de GitHub Actions, y marca cuál configuración resultó
más rápida para las 600 filas del dataset ampliado. Como referencia de qué esperar (a
confirmar con la corrida real, no asumir sin verificar):

| Configuración | Job | Qué se espera observar |
|---|---|---|
| `hilos=1` (secuencial) | multi-hilo | Debería ser la más lenta de las variantes de hilos: sin paralelismo, pero también sin overhead de coordinación entre hilos |
| `hilos=3` (línea base exigida) | multi-hilo | Debería mejorar sobre `hilos=1`; es el mínimo exigido por las instrucciones específicas |
| `hilos=6` | multi-hilo | Con solo 600 filas y chunk=5 (120 chunks), más hilos que trabajo disponible puede no mejorar sobre `hilos=3`, o incluso empeorar por overhead de coordinación/contención de conexiones |
| `chunk-size` pequeño (1) vs. grande (50) | multi-hilo | Chunks muy pequeños multiplican los commits (overhead de transacción); chunks muy grandes reducen el paralelismo real percibido y agrandan el radio de una eventual falla/reintento |
| `grid-size` 1, 2, 4, 6 | particionado | Cada partición abre su propio archivo y su propia conexión: para un archivo de 600 filas, el overhead de abrir N streams puede pesar más que el procesamiento en sí a partir de cierto `grid-size` — la corrida real debe mostrar en qué punto deja de compensar |

**Conclusión (completar con los números reales de tu corrida):** la configuración óptima
para este volumen de datos (600 filas) es `______`, porque `______`. Para un volumen de datos
significativamente mayor (por ejemplo, miles o millones de filas, como sería el caso real de
transacciones diarias de un banco), se esperaría que el paralelismo (por hilos o por
particiones) compense de forma más clara el overhead de coordinación — este benchmark
documenta el comportamiento observado en el volumen de prueba disponible, no una regla
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
saludable, compila con Maven, corre los 3 Jobs, corre una cuarta vez el Job de intereses
contra el dataset original (para mostrar la rama `REVISION_REQUERIDA` del decider), y ejecuta
4 consultas SQL de verificación. Todo queda guardado con timestamp en
`evidencias/corrida-<fecha-hora>/`.

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

# rama alterna del decider (dataset original, ~60% de registros invalidos):
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
| Log de `transaccionesDiariasJob` | Chunks + 3 hilos + resumen de anomalías |
| Log de `interesesMensualesJob` | Cálculo de intereses + upsert |
| Log de `estadosCuentaAnualesJob` | Agregación anual por cuenta |
| Log de la corrida con `sample-data/intereses.csv` | Rama `REVISION_REQUERIDA` del decider |
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
- En la corrida contra `sample-data/`, la línea
  `Porcentaje de omision (...) supera el umbral` seguida de `REVISION_REQUERIDA`: el
  `JobExecutionDecider` en acción (criterio "políticas de finalización y re-ejecución").
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
