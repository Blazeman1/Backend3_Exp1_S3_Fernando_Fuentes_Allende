-- Tablas de negocio del Banco XYZ (Exp1 - Semana 2).
-- Las tablas de metadata de Spring Batch (BATCH_JOB_INSTANCE, BATCH_JOB_EXECUTION,
-- BATCH_STEP_EXECUTION, etc.) se crean automaticamente gracias a
-- spring.batch.jdbc.initialize-schema=always, por lo que no se declaran aqui.

-- ============================================================
-- Job 1: Reporte de Transacciones Diarias
-- ============================================================
CREATE TABLE IF NOT EXISTS transacciones_procesadas (
    id                  BIGSERIAL PRIMARY KEY,
    id_origen           BIGINT NOT NULL,
    fecha               DATE NOT NULL,
    monto               NUMERIC(15,2) NOT NULL,
    tipo                VARCHAR(20) NOT NULL,
    es_anomalia         BOOLEAN NOT NULL DEFAULT FALSE,
    motivo_anomalia     VARCHAR(200),
    fecha_procesamiento TIMESTAMP NOT NULL DEFAULT now(),
    -- Restriccion de negocio que permite detectar transacciones duplicadas del CSV legacy:
    -- si dos filas comparten fecha, monto y tipo, la segunda se omite (SkipPolicy) en vez de
    -- insertarse dos veces.
    CONSTRAINT uq_transaccion_natural UNIQUE (fecha, monto, tipo)
);

CREATE TABLE IF NOT EXISTS resumen_transacciones_diarias (
    id                    BIGSERIAL PRIMARY KEY,
    fecha_generacion      TIMESTAMP NOT NULL DEFAULT now(),
    total_procesadas      INTEGER NOT NULL,
    total_anomalias       INTEGER NOT NULL,
    total_omitidas        INTEGER NOT NULL,
    monto_total_creditos  NUMERIC(18,2) NOT NULL,
    monto_total_debitos   NUMERIC(18,2) NOT NULL
);

-- ============================================================
-- Job 2: Calculo de Intereses Mensuales
-- ============================================================
CREATE TABLE IF NOT EXISTS cuentas_interes (
    cuenta_id             BIGINT PRIMARY KEY,
    nombre                VARCHAR(150) NOT NULL,
    tipo                  VARCHAR(20) NOT NULL,
    edad                  INTEGER NOT NULL,
    saldo_inicial         NUMERIC(15,2) NOT NULL,
    tasa_interes_mensual  NUMERIC(6,4) NOT NULL,
    interes_calculado     NUMERIC(15,2) NOT NULL,
    saldo_final           NUMERIC(15,2) NOT NULL,
    fecha_procesamiento   TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Job 3: Generacion de Estados de Cuenta Anuales
-- ============================================================
CREATE TABLE IF NOT EXISTS cuentas_anuales_movimientos (
    id                  BIGSERIAL PRIMARY KEY,
    cuenta_id           BIGINT NOT NULL,
    fecha               DATE NOT NULL,
    tipo_movimiento     VARCHAR(20) NOT NULL,
    monto               NUMERIC(15,2) NOT NULL,
    descripcion         VARCHAR(200) NOT NULL,
    es_anomalia         BOOLEAN NOT NULL DEFAULT FALSE,
    motivo_anomalia     VARCHAR(200),
    fecha_procesamiento TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS estados_cuenta_anuales (
    cuenta_id             BIGINT PRIMARY KEY,
    anio                  INTEGER NOT NULL,
    total_depositos       NUMERIC(18,2) NOT NULL,
    total_retiros         NUMERIC(18,2) NOT NULL,
    total_compras         NUMERIC(18,2) NOT NULL,
    saldo_neto            NUMERIC(18,2) NOT NULL,
    cantidad_movimientos  INTEGER NOT NULL,
    cantidad_anomalias    INTEGER NOT NULL,
    fecha_generacion      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_movimientos_cuenta ON cuentas_anuales_movimientos (cuenta_id);
