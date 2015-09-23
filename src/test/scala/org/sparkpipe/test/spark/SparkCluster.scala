package org.sparkpipe.test.spark

import scala.io.Source
import scala.util.{Try, Success, Failure}
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import org.sparkpipe.test.util.Base

trait SparkCluster extends SparkLocal with Base {
    /** Loading Spark configuration for cluster. */
    final protected def clusterConf(): SparkConf = {
        // OS check
        val system = System.getProperty("os.name").toLowerCase()
        require(system.startsWith("mac") || system.startsWith("linux"),
            "Spark Cluster runs only on Unix-like OS")

        // throw exception if SPARK_HOME is not set
        val SPARK_HOME = Option(System.getenv("SPARK_HOME")) match {
            case Some(opt) => opt
            case None => throw new UnsupportedOperationException(
                "Spark Cluster mode requires SPARK_HOME to be set"
            )
        }

        // throw exception if SPARK_MASTER_ADDRESS is not set
        val SPARK_MASTER_ADDRESS = Option(System.getenv("SPARK_MASTER_ADDRESS")) match {
            case Some(opt) => opt
            case None => throw new UnsupportedOperationException(
                "Spark Cluster mode requires SPARK_MASTER_ADDRESS to be set"
            )
        }

        // read internal classes to pass to executor JVMs
        val SPARK_PREPEND_CLASSES = Seq(
            targetDirectory() + / + "streams" + / + "runtime" + / + "exportedProducts" +
            / + "$global" + / + "streams" + / + "export",
            targetDirectory() + / + "streams" + / + "test" + / + "exportedProducts" +
            / + "$global" + / + "streams" + / + "export"
        ).flatMap(filepath =>
            Source.fromFile(filepath).getLines()
        ).mkString(`:`)

        // in case of dependencies pass them to executor JVMs as libs
        val SPARK_MANAGED_DEPENDENCIES = Seq(
            targetDirectory() + / + "streams" + / + "compile" + / + "dependencyClasspath" +
            / + "$global" + / + "streams" + / + "export"
        ).flatMap(filepath =>
            Source.fromFile(filepath).getLines()
        ).mkString(`:`)

        new SparkConf().
           setMaster(SPARK_MASTER_ADDRESS).
           setAppName("spark-cluster-test").
           set("spark.driver.memory", "1g").
           set("spark.executor.memory", "1g").
           set("spark.executorEnv.SPARK_HOME", SPARK_HOME).
           set("spark.executor.extraClassPath", SPARK_PREPEND_CLASSES).
           set("spark.executor.extraLibraryPath", SPARK_MANAGED_DEPENDENCIES)
    }

    /**
     * Tries loading configuration for cluster.
     * `fallback` option allows to load local mode instead in case of failure.
     */
    private def tryConf(fallback: Boolean = true): SparkConf = Try(clusterConf()) match {
        case Success(conf) => conf
        case Failure(e) => {
            if (fallback) {
                println("[info] Spark cluster failed to load: " + e.getMessage())
                println("[info] Using Spark local context instead")
                localConf()
            } else {
                throw e
            }
        }
    }

    override def startSparkContext() {
        setLoggingLevel(Level.ERROR)
        val conf = tryConf(fallback=true)
        _sc = new SparkContext(conf)
    }
}
