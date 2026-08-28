package com.bancoxyz.batch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades de configuracion del proyecto, externalizadas en {@code application.yml} bajo
 * el prefijo {@code batch}. Centralizar aqui el tamano de chunk, el numero de hilos, los
 * limites de omision/reintento y las rutas de los CSV de entrada permite ajustar el
 * comportamiento del sistema sin recompilar (criterio de la pauta: "Optimiza los recursos del
 * sistema configurando parametros para evitar problemas de rendimiento").
 */
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {

    /** Tamano de chunk exigido por las instrucciones especificas: 5. */
    private int chunkSize = 5;

    /** Numero de hilos de ejecucion paralela exigido por las instrucciones especificas: 3. */
    private int hilos = 3;

    /**
     * Semana 3: cantidad de particiones (grid size) para el modo particionado del Job de
     * transacciones diarias. A diferencia de {@code hilos} (paralelismo dentro de un unico
     * Step), este valor determina en cuantas ejecuciones de Step independientes se divide el
     * archivo de transacciones. Configurable por linea de comandos
     * ({@code --batch.grid-size=N}) para poder comparar distintas configuraciones (ver
     * benchmark en GUIA_EJECUCION.md).
     */
    private int gridSize = 4;

    /**
     * Cantidad maxima de registros que un Step puede omitir antes de fallar. Es un circuito de
     * seguridad ("no perder datos en silencio si algo se sale de madre"), NO el mecanismo que
     * decide si la calidad de los datos es aceptable -esa evaluacion, por porcentaje, la hace
     * {@code ControlCalidadDecider} despues de que el Step termina-. Por eso este limite se deja
     * bastante por encima de la tasa de omision real observada contra el dataset oficial de la
     * semana 3 (verificada programaticamente: 21.5% en transacciones, 83.6% en intereses, 6% en
     * cuentas anuales sobre 1000 filas cada uno): si se dejara en un valor bajo (como el 200 de
     * la semana 2, pensado para un dataset de 300-900 filas con ~15-20% de error), el Step
     * fallaria de forma abrupta antes de terminar de leer el archivo, impidiendo que el decider
     * llegara siquiera a evaluar el porcentaje real y a derivar correctamente a revision manual.
     */
    private long limiteOmisiones = 900;

    /** Numero maximo de reintentos ante fallas transitorias de infraestructura. */
    private int maximoReintentos = 3;

    /** Porcentaje de omision (0-100) a partir del cual el ControlCalidadDecider deriva a revision manual. */
    private double umbralCalidadPorcentaje = 20.0;

    private Rutas rutas = new Rutas();

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getHilos() {
        return hilos;
    }

    public void setHilos(int hilos) {
        this.hilos = hilos;
    }

    public int getGridSize() {
        return gridSize;
    }

    public void setGridSize(int gridSize) {
        this.gridSize = gridSize;
    }

    public long getLimiteOmisiones() {
        return limiteOmisiones;
    }

    public void setLimiteOmisiones(long limiteOmisiones) {
        this.limiteOmisiones = limiteOmisiones;
    }

    public int getMaximoReintentos() {
        return maximoReintentos;
    }

    public void setMaximoReintentos(int maximoReintentos) {
        this.maximoReintentos = maximoReintentos;
    }

    public double getUmbralCalidadPorcentaje() {
        return umbralCalidadPorcentaje;
    }

    public void setUmbralCalidadPorcentaje(double umbralCalidadPorcentaje) {
        this.umbralCalidadPorcentaje = umbralCalidadPorcentaje;
    }

    public Rutas getRutas() {
        return rutas;
    }

    public void setRutas(Rutas rutas) {
        this.rutas = rutas;
    }

    /** Rutas (recursos Spring: {@code classpath:...} o {@code file:...}) de los CSV de origen. */
    public static class Rutas {
        private String transacciones = "classpath:data/transacciones.csv";
        private String intereses = "classpath:data/intereses.csv";
        private String cuentasAnuales = "classpath:data/cuentas_anuales.csv";

        public String getTransacciones() {
            return transacciones;
        }

        public void setTransacciones(String transacciones) {
            this.transacciones = transacciones;
        }

        public String getIntereses() {
            return intereses;
        }

        public void setIntereses(String intereses) {
            this.intereses = intereses;
        }

        public String getCuentasAnuales() {
            return cuentasAnuales;
        }

        public void setCuentasAnuales(String cuentasAnuales) {
            this.cuentasAnuales = cuentasAnuales;
        }
    }
}
