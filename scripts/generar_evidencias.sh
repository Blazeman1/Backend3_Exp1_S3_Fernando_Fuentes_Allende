#!/usr/bin/env bash
# Automatiza la corrida real de los 3 Jobs y deja las salidas de consola en evidencias/,
# listas para adjuntar a la entrega. Pensado para ejecutarse en TU máquina (o un laboratorio
# de Duoc) con Docker y Maven instalados — no funciona dentro de un entorno sin acceso a
# Maven Central / Docker Hub.
#
# Uso:
#   chmod +x scripts/generar_evidencias.sh
#   ./scripts/generar_evidencias.sh
set -euo pipefail
cd "$(dirname "$0")/.."

TS="$(date +%Y%m%d-%H%M%S)"
OUT="evidencias/corrida-${TS}"
mkdir -p "$OUT"

echo "==> [1/7] Levantando PostgreSQL con docker compose..."
docker compose up -d
echo "==> Esperando a que PostgreSQL esté saludable..."
for i in $(seq 1 30); do
  if docker exec banco-xyz-postgres pg_isready -U banco_xyz -d banco_xyz >/dev/null 2>&1; then
    echo "    PostgreSQL listo."
    break
  fi
  sleep 2
done

echo "==> [2/7] Compilando el proyecto (mvn clean package)..."
mvn -q clean package | tee "$OUT/01-build.log"

JAR="target/banco-xyz-batch-1.0.0.jar"

echo "==> [3/7] Ejecutando Job 1: transaccionesDiariasJob..."
java -jar "$JAR" transacciones | tee "$OUT/02-job-transacciones.log"

echo "==> [4/7] Ejecutando Job 2: interesesMensualesJob..."
java -jar "$JAR" intereses | tee "$OUT/03-job-intereses.log"

echo "==> [5/7] Ejecutando Job 3: estadosCuentaAnualesJob..."
java -jar "$JAR" cuentas-anuales | tee "$OUT/04-job-cuentas-anuales.log"

echo "==> [6/7] Ejecutando Job 2 contra el dataset original (para ver la rama REVISION_REQUERIDA)..."
java -jar "$JAR" intereses --batch.rutas.intereses=classpath:sample-data/intereses.csv \
  | tee "$OUT/05-job-intereses-sample-data.log"

echo "==> [7/7] Consultando los resultados en la base de datos..."
{
  echo "=== resumen_transacciones_diarias ==="
  docker exec banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM resumen_transacciones_diarias;"
  echo
  echo "=== cuentas_interes (10 primeras filas) ==="
  docker exec banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM cuentas_interes LIMIT 10;"
  echo
  echo "=== estados_cuenta_anuales (10 primeras filas) ==="
  docker exec banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT * FROM estados_cuenta_anuales LIMIT 10;"
  echo
  echo "=== anomalias detectadas por Job 1 ==="
  docker exec banco-xyz-postgres psql -U banco_xyz -d banco_xyz -c "SELECT es_anomalia, COUNT(*) FROM transacciones_procesadas GROUP BY es_anomalia;"
} | tee "$OUT/06-consultas-sql.log"

echo
echo "Listo. Evidencias guardadas en: $OUT"
echo "Revisa especialmente:"
echo "  - '[Batch-Thread-' en 02/03/04 para confirmar el procesamiento en paralelo (3 hilos)"
echo "  - 'RendimientoStepListener' para las métricas de throughput por Step"
echo "  - 05-job-intereses-sample-data.log debería terminar en la rama REVISION_REQUERIDA"
