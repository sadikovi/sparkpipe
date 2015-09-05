package org.sparkpipe.test

import org.apache.spark.sql.SQLContext
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
    test("Local Spark context init and work") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }

    test("Local Spark context init sqlContext") {
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
    test("Spark local cluster context init and work") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }
}

class SparkClusterSpec extends UnitTestSpec with SparkCluster {
    /** tests Spark cluster if available */
    test("Spark cluster context init and work") {
        startSparkContext()
        val a = sc.parallelize(Array(1, 2, 3, 4))
        a.collect.sortWith(_ < _) should be (Array(1, 2, 3, 4))
        stopSparkContext()
    }
}
