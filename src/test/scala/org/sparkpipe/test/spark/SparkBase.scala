package org.sparkpipe.test.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/** General Spark base */
private[spark] trait SparkBase {
    @transient private[spark] var _sc: SparkContext = null

    /** Start (or init) Spark context. */
    def startSparkContext() {
        // stop previous Spark context
        stopSparkContext()
        _sc = new SparkContext()
    }

    /** Stop Spark context. */
    def stopSparkContext() {
        if (_sc != null) {
            _sc.stop()
        }
        _sc = null
    }

    /**
     * Set logging level globally for all.
     * Supported log levels:
     *      Level.OFF
     *      Level.ERROR
     *      Level.WARN
     *      Level.INFO
     * @param level logging level
     */
    def setLoggingLevel(level:Level) {
        Logger.getLogger("org").setLevel(level)
        Logger.getLogger("akka").setLevel(level)
        Logger.getRootLogger().setLevel(level)
    }

    /** Returns Spark context. */
    def sc: SparkContext = _sc
}
