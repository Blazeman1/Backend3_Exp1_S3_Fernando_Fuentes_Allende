#!/usr/bin/env bash
# Semana 3: ejecuta el Job particionado y un benchmark comparativo variando parametros
# (hilos, chunk-size, grid-size) del Job de transacciones diarias, dejando la evidencia
# numerica (duracion total de cada corrida, via JobResumenListener) en evidencias/.
#
# Requiere PostgreSQL ya levantado (docker compose up -d) y el proyecto ya compilado
# (mvn clean package) - normalmente se corre justo despues de scripts/generar_evidencias.sh,
# reutilizando el mismo contenedor y el mismo JAR.
set -euo pipefail
cd "$(dirname "$0")/.."

TS="$(date +%Y%m%d-%H%M%S)"
OUT="evidencias/benchmark-${TS}"
mkdir -p "$OUT"

JAR="target/banco-xyz-batch-1.0.0.jar"
if [ ! -f "$JAR" ]; then
  echo "==> No se encontro $JAR. Compilando primero..."
  mvn -q clean package
fi

if ! docker exec banco-xyz-postgres pg_isready -U banco_xyz -d banco_xyz >/dev/null 2>&1; then
  echo "==> Levantando PostgreSQL con docker compose..."
  docker compose up -d
  for i in $(seq 1 30); do
    if docker exec banco-xyz-postgres pg_isready -U banco_xyz -d banco_xyz >/dev/null 2>&1; then
      echo "    PostgreSQL listo."
      break
    fi
    sleep 2
  done
fi

echo "==> [1/4] Ejecutando Job particionado (grid-size por defecto = 4)..."
java -jar "$JAR" transacciones-particionado | tee "$OUT/01-job-transacciones-particionado.log"

echo "==> [2/4] Benchmark: variando 'hilos' del Job multi-hilo (chunk-size fijo = 5)..."
: > "$OUT/02-benchmark-hilos.log"
for hilos in 1 3 6; do
  echo "=== hilos=${hilos} chunk=5 ===" | tee -a "$OUT/02-benchmark-hilos.log"
  java -jar "$JAR" transacciones --batch.hilos="${hilos}" --runId="bench-hilos-${hilos}-${TS}" \
    | tee -a "$OUT/02-benchmark-hilos.log"
done

echo "==> [3/4] Benchmark: variando 'chunk-size' del Job multi-hilo (hilos fijo = 3)..."
: > "$OUT/03-benchmark-chunk.log"
for chunk in 1 5 20 50; do
  echo "=== hilos=3 chunk=${chunk} ===" | tee -a "$OUT/03-benchmark-chunk.log"
  java -jar "$JAR" transacciones --batch.chunk-size="${chunk}" --runId="bench-chunk-${chunk}-${TS}" \
    | tee -a "$OUT/03-benchmark-chunk.log"
done

echo "==> [4/4] Benchmark: variando 'grid-size' del Job particionado (chunk-size fijo = 5)..."
: > "$OUT/04-benchmark-gridsize.log"
for grid in 1 2 4 6; do
  echo "=== gridSize=${grid} chunk=5 ===" | tee -a "$OUT/04-benchmark-gridsize.log"
  java -jar "$JAR" transacciones-particionado --batch.grid-size="${grid}" --runId="bench-grid-${grid}-${TS}" \
    | tee -a "$OUT/04-benchmark-gridsize.log"
done

echo "==> Consolidando resumen del benchmark..."
{
  echo "=== Resumen de duracion total por corrida (grep de 'FIN JOB' y encabezados '===') ==="
  grep -E "^=== |FIN JOB" "$OUT/02-benchmark-hilos.log" "$OUT/03-benchmark-chunk.log" "$OUT/04-benchmark-gridsize.log"
} | tee "$OUT/05-resumen-benchmark.log"

echo
echo "Listo. Evidencias del benchmark guardadas en: $OUT"
echo "Revisa especialmente '05-resumen-benchmark.log': ahi estan, en orden, la configuracion"
echo "probada y la duracion total (ms) que dejo JobResumenListener para cada una."
