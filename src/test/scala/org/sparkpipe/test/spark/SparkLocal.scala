package org.sparkpipe.test.spark

import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.sparkpipe.test.util.UnitTestSpec

/** Spark context with master "local[4]" */
trait SparkLocal extends SparkBase {
    /** Loading Spark configuration for local mode */
    final protected def localConf(): SparkConf = {
        new SparkConf().
            setMaster("local[4]").
            setAppName("spark-local-test").
            set("spark.driver.memory", "1g").
            set("spark.executor.memory", "2g")
    }

    override def startSparkContext() {
        setLoggingLevel(Level.ERROR)
        val conf = localConf()
        _sc = new SparkContext(conf)
    }
}
