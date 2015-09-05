package org.sparkpipe.test.spark

import scala.io.Source
import scala.util.{Try, Success, Failure}
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}
import org.sparkpipe.test.util.Base

trait SparkLocalCluster extends SparkLocal with Base {
    /** loading Spark configuration for local cluster. */
    final protected def localClusterConf(): SparkConf = {
        // OS check
        val system = System.getProperty("os.name").toLowerCase()
        require(system.startsWith("mac") || system.startsWith("linux"),
            "Spark local cluster runs only on Unix-like OS")

        // throw exception if SPARK_HOME is not set
        val SPARK_HOME = Option(System.getenv("SPARK_HOME")) match {
            case Some(opt) => opt
            case None => throw new UnsupportedOperationException(
                "Spark Local cluster mode requires SPARK_HOME to be set"
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
           setMaster("local-cluster[4, 1, 1024]").
           setAppName("spark-local-cluster-test").
           set("spark.driver.memory", "1g").
           set("spark.executor.memory", "1g").
           set("spark.executorEnv.SPARK_HOME", SPARK_HOME).
           set("spark.executor.extraClassPath", SPARK_PREPEND_CLASSES).
           set("spark.executor.extraLibraryPath", SPARK_MANAGED_DEPENDENCIES)
    }

    /**
     * tries loading configuration for local cluster. Can load local mode in case of failure, if
     * `fallback` options is set to true.
     */
    private def tryConf(fallback: Boolean = true): SparkConf = Try(localClusterConf()) match {
        case Success(conf) => conf
        case Failure(e) => {
            if (fallback) {
                println("[info] Spark local cluster failed to load: " + e.getMessage())
                println("[info] Using Spark local context instead")
                localConf()
            } else {
                throw e
            }
        }
    }

    override def startSparkContext() {
        // set error level as OFF, because of FileAppender error
        setLoggingLevel(Level.OFF)
        val conf = tryConf(fallback=true)
        _sc = new SparkContext(conf)
    }
}
