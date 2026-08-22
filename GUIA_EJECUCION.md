# Guía de ejecución — Banco XYZ Batch

Esta guía te lleva de "el código está listo" a "tengo la evidencia de ejecución en la mano".
Hay dos rutas; con seguir **una** de las dos basta, pero puedes hacer ambas.

- **Ruta A — GitHub Actions**: no instalas nada, todo corre en la nube de GitHub. Recomendada
  si no tienes Docker o Maven configurados localmente, o si prefieres evidencia reproducible
  que cualquier persona puede volver a generar abriendo el repositorio.
- **Ruta B — Local**: para quienes ya tienen Docker y Maven instalados y quieren ver la
  ejecución en su propia terminal.

## Antes de empezar

- El proyecto (`Exp1_S2_Grupo18.zip`) descomprimido en tu computador.
- Una cuenta de GitHub propia (Ruta A) y/o Docker + JDK 21 + Maven 3.9+ instalados (Ruta B).
- 10-15 minutos.

---

## Ruta A — GitHub Actions (recomendada)

### 1. Sube el proyecto a tu cuenta de GitHub

Si todavía no lo hiciste:

```bash
cd Exp1_S2_Grupo18
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
3. Abre la ejecución más reciente, llamada **"Evidencia de ejecución - Banco XYZ Batch"**.
4. Tarda 2 a 3 minutos. Verás 10 pasos ejecutándose en orden: checkout, JDK, compilación,
   los 3 Jobs, la corrida de la rama alterna del decider, las consultas SQL y la publicación
   del artefacto.
5. Si quieres relanzarlo sin hacer otro commit, usa el botón **Run workflow** (arriba a la
   derecha de esa misma pantalla).

### 4. Descarga la evidencia

1. Cuando el run termine (ícono verde ✓), baja hasta el final de esa página.
2. En la sección **Artifacts**, descarga `evidencia-ejecucion-banco-xyz.zip`.
3. Descomprímelo: trae 6 archivos `.log` con la salida de consola completa —
   `evidencia-01-build.log` hasta `evidencia-06-consultas-sql.log`.
4. Mueve esos 6 archivos a la carpeta `evidencias/` del proyecto.
5. Toma una captura de pantalla de la página de Actions en verde (el resumen del run, con
   los 10 pasos marcados como completados) y guárdala también en `evidencias/`.

Eso es todo: ya tienes logs de consola reales más una captura de pantalla, exactamente lo
que piden las instrucciones específicas.

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
cd Exp1_S2_Grupo18
chmod +x scripts/generar_evidencias.sh
./scripts/generar_evidencias.sh
```

El script hace, en orden: levanta PostgreSQL con `docker compose`, espera a que esté
saludable, compila con Maven, corre los 3 Jobs, corre una cuarta vez el Job de intereses
contra el dataset original (para mostrar la rama `REVISION_REQUERIDA` del decider), y ejecuta
4 consultas SQL de verificación. Todo queda guardado con timestamp en
`evidencias/corrida-<fecha-hora>/`.

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
| Captura de la pestaña Actions en verde (Ruta A) | Evidencia reproducible, visible para cualquiera |

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

## Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| `mvn clean package` falla por dependencias | Sin conexión a Maven Central | Revisa tu red; en la Ruta A esto no ocurre porque el runner de GitHub sí tiene acceso |
| `Connection refused` a PostgreSQL | El contenedor aún no está listo | Espera unos segundos más, o revisa `docker ps` y `docker logs banco-xyz-postgres` |
| Puerto 5432 ya en uso | Ya tienes un PostgreSQL corriendo localmente | Detén ese servicio o cambia el mapeo de puertos en `docker-compose.yml` |
| El workflow no aparece en Actions | El repo no tiene Actions habilitado, o el push fue a una rama distinta de `main`/`master` | Revisa Settings → Actions esté habilitado, y que el push haya sido a `main` |
| El Job 1 termina `COMPLETED` en el log pero el step de Actions se queda "corriendo" para siempre y nunca llega al Job 2 | La JVM no cierra sola: el `ThreadPoolTaskExecutor` (3 hilos, `corePoolSize == maxPoolSize`) crea hilos no-daemon que quedan vivos esperando tareas aunque el Job ya haya terminado | Ya corregido en `BancoXyzBatchApplication`: `main()` ahora cierra el contexto y fuerza la salida con `System.exit(SpringApplication.exit(contexto, ...))`. Si ves este síntoma, verifica que tu copia tenga ese cambio (commit "Corregir cierre de la JVM...") |
