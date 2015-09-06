package org.sparkpipe.test

import org.apache.spark.sql.SQLContext
import org.jblas.DoubleMatrix
import org.sparkpipe.test.spark.{SparkLocal, SparkLocalCluster, SparkCluster}
import org.sparkpipe.test.util.UnitTestSpec

class BaseSpec extends UnitTestSpec {
    /** prints out paths of the project */
    test("Test spec should report directories") {
        println("base dir: " + baseDirectory())
        println("main dir: " + mainDirectory())
        println("test dir: " + testDirectory())
        println("target dir: " + targetDirectory())
    }
}

class SparkLocalSpec extends UnitTestSpec with SparkLocal {
    /** tests local Spark context */
    test("Local context initialisation and work") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }

    test("Local sqlContext initialisation") {
        startSparkContext()
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val a = sc.parallelize(Array(1, 2, 3, 4)).toDF("index")
        a.registerTempTable("indices")
        sqlContext.sql("select index from indices").count should be (4)
        stopSparkContext()
    }
}

class SparkLocalClusterSpec extends UnitTestSpec with SparkLocalCluster {
    /** tests Spark cluster if available */
    test("Local cluster context initialisation") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }
}

class SparkClusterSpec extends UnitTestSpec with SparkCluster {
    /** tests Spark cluster if available */
    test("Cluster context initialisation") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }

    test("Spark cluster should load class files and libs") {
        startSparkContext()
        val a = sc.parallelize(
            Array(
                Array(1.0, 2.0, 3.0),
                Array(4.0, 5.0, 6.0),
                Array(7.0, 8.0, 9.0)
            )
        )
        val matrix = new DoubleMatrix(a.collect())
        matrix.isSquare should be (true)
        stopSparkContext()
    }
}
