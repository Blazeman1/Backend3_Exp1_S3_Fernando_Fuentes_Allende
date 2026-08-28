#!/usr/bin/env python3
"""
NOTA (Semana 3): este script queda como referencia historica de las semanas 1-2, cuando el
dataset oficial de esa epoca (carpeta data/semana_2 del repositorio legacy) solo traia 8-10
filas por archivo -insuficiente para evidenciar de forma convincente chunks + multithreading- y
por eso se generaba aqui una version sintetica ampliada.

Para la actividad sumativa individual de la semana 3, el dataset oficial correspondiente
(https://github.com/KariVillagran/bank_legacy_data, carpeta data/semana_3) YA trae 1000 filas
por archivo, con una tasa de error real (no fabricada) suficiente por si sola para evidenciar
el procesamiento en chunks, el multithreading/particionado, y las politicas de tolerancia a
fallos. Por eso este script YA NO se ejecuta como parte del flujo normal de la semana 3: los
archivos en src/main/resources/data/ son ahora una copia directa, sin modificar, del dataset
oficial de semana_3, y los de src/main/resources/sample-data/ son subconjuntos curados
(filtrados programaticamente, sin inventar valores) de ese mismo dataset oficial, usados para
demostrar puntualmente la rama CALIDAD_OK del ControlCalidadDecider (ver README, seccion 1).

Genera datasets sinteticos, pero mas grandes, para los 3 procesos batch del Banco XYZ,
preservando los MISMOS patrones de calidad de datos descritos en el README del repositorio
oficial: montos negativos/cero/vacios, fechas en dos formatos, edades fuera de rango, tipos
invalidos, registros duplicados y descripciones faltantes.

Uso (solo si se quiere reproducir el dataset sintetico historico de la semana 2):
python3 scripts/generar_datos.py
"""
import csv
import random
from pathlib import Path

random.seed(42)  # reproducibilidad: la misma semilla siempre genera el mismo dataset

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / "src" / "main" / "resources" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fecha_str(dia_del_anio, formato_legacy=False):
    from datetime import date, timedelta
    f = date(2024, 1, 1) + timedelta(days=dia_del_anio - 1)
    return f.strftime("%Y/%m/%d") if formato_legacy else f.strftime("%Y-%m-%d")


# ------------------------------------------------------------------
# transacciones.csv (Job 1: Reporte de Transacciones Diarias)
# ------------------------------------------------------------------
def generar_transacciones(n=600):
    filas = []
    vistas = set()
    id_actual = 1
    while len(filas) < n:
        tipo_roll = random.random()
        if tipo_roll < 0.47:
            tipo = "debito"
        elif tipo_roll < 0.94:
            tipo = "credito"
        else:
            tipo = random.choice(["invalido", "x", "otro"])  # tipo no reconocido -> anomalia (se conserva)

        dia = random.randint(1, 31)
        fecha = fecha_str(dia, formato_legacy=(random.random() < 0.10))  # 10% formato legacy corregible

        monto_roll = random.random()
        if monto_roll < 0.03:
            monto = ""  # monto vacio -> se omite (irrecuperable)
        elif monto_roll < 0.10:
            monto = str(-round(random.uniform(10, 3000), 2))  # negativo -> anomalia (se conserva)
        elif monto_roll < 0.12:
            monto = "0"  # cero -> anomalia (se conserva)
        else:
            monto = str(round(random.uniform(10, 5000), 2))

        clave = (fecha, monto, tipo)
        # ~3% duplicados exactos a proposito, para ejercitar la restriccion UNIQUE + skip en escritura
        if filas and random.random() < 0.03:
            origen = random.choice(filas)
            filas.append({"id": id_actual, "fecha": origen["fecha"], "monto": origen["monto"], "tipo": origen["tipo"]})
        else:
            filas.append({"id": id_actual, "fecha": fecha, "monto": monto, "tipo": tipo})
        vistas.add(clave)
        id_actual += 1

    with open(OUT_DIR / "transacciones.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "fecha", "monto", "tipo"])
        writer.writeheader()
        writer.writerows(filas)
    print(f"transacciones.csv generado con {len(filas)} filas")


# ------------------------------------------------------------------
# intereses.csv (Job 2: Calculo de Intereses Mensuales)
# ------------------------------------------------------------------
NOMBRES = ["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown", "Charlie Green",
           "Diana Prince", "Steve Rogers", "Maria Gonzalez", "Pedro Alvarez", "Karen Villagran",
           "Ignacio Villarroel", "Camila Rojas", "Felipe Soto", "Valentina Diaz", "Andres Castro"]


def generar_intereses(n=300):
    filas = []
    cuenta_id = 1000
    contenido_previo = None
    while len(filas) < n:
        cuenta_id += 1
        nombre = random.choice(NOMBRES)

        tipo_roll = random.random()
        if tipo_roll < 0.85:
            tipo = random.choice(["ahorro", "prestamo"])
        else:
            tipo = random.choice(["hipoteca", "-1", "", "otro"])  # fuera de alcance -> omitido

        edad_roll = random.random()
        if edad_roll < 0.06:
            edad = ""  # vacia -> omitida
        elif edad_roll < 0.11:
            edad = str(random.choice([5, 12, 95, 110, 130]))  # fuera de rango -> omitida
        else:
            edad = str(random.randint(18, 90))

        saldo_roll = random.random()
        if saldo_roll < 0.05:
            saldo = ""  # vacio -> omitido
        elif saldo_roll < 0.07:
            saldo = str(-round(random.uniform(100, 5000), 2))  # negativo -> omitido
        else:
            saldo = str(round(random.uniform(300, 60000), 2))

        # ~4% registros con el mismo contenido de negocio que el anterior (duplicado logico)
        if contenido_previo and random.random() < 0.04:
            nombre, saldo, edad, tipo = contenido_previo
        else:
            contenido_previo = (nombre, saldo, edad, tipo)

        filas.append({"cuenta_id": cuenta_id, "nombre": nombre, "saldo": saldo, "edad": edad, "tipo": tipo})

    with open(OUT_DIR / "intereses.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cuenta_id", "nombre", "saldo", "edad", "tipo"])
        writer.writeheader()
        writer.writerows(filas)
    print(f"intereses.csv generado con {len(filas)} filas")


# ------------------------------------------------------------------
# cuentas_anuales.csv (Job 3: Generacion de Estados de Cuenta Anuales)
# ------------------------------------------------------------------
DESCRIPCIONES = {
    "deposito": ["Ingreso mensual", "Deposito en sucursal", "Transferencia recibida", "Ingreso extra"],
    "retiro": ["Retiro parcial", "Retiro cajero", "Retiro en sucursal"],
    "compra": ["Compra en tienda", "Compra online", "Pago de servicio"],
}


def generar_cuentas_anuales(n_cuentas=150, movimientos_por_cuenta=(4, 8)):
    filas = []
    for cuenta_id in range(2000, 2000 + n_cuentas):
        cantidad = random.randint(*movimientos_por_cuenta)
        for _ in range(cantidad):
            tipo = random.choices(["deposito", "retiro", "compra"], weights=[0.45, 0.30, 0.25])[0]
            dia = random.randint(1, 365)
            fecha = fecha_str(dia, formato_legacy=(random.random() < 0.15))  # 15% formato legacy corregible

            base = round(random.uniform(20, 3000), 2)
            monto_roll = random.random()
            if tipo == "deposito":
                monto = base if monto_roll >= 0.10 else -base  # 10% con signo invertido -> se corrige
            else:
                monto = -base if monto_roll >= 0.10 else base  # 10% con signo invertido -> se corrige

            descripcion = random.choice(DESCRIPCIONES[tipo])
            if random.random() < 0.08:
                descripcion = ""  # 8% vacia -> se corrige con valor por defecto

            filas.append({
                "cuenta_id": cuenta_id,
                "fecha": fecha,
                "transaccion": tipo,
                "monto": monto,
                "descripcion": descripcion,
            })

    random.shuffle(filas)
    with open(OUT_DIR / "cuentas_anuales.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cuenta_id", "fecha", "transaccion", "monto", "descripcion"])
        writer.writeheader()
        writer.writerows(filas)
    print(f"cuentas_anuales.csv generado con {len(filas)} filas para {n_cuentas} cuentas")


if __name__ == "__main__":
    generar_transacciones()
    generar_intereses()
    generar_cuentas_anuales()
