package org.sparkpipe.rdd

import org.scalatest._
import org.sparkpipe.rdd.implicits._
import org.sparkpipe.test.spark.SparkLocal
import org.sparkpipe.test.util.UnitTestSpec

class FileStatisticsRDDSpec extends UnitTestSpec with SparkLocal with BeforeAndAfterAll {
    val textFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
        "rdd" + / + "*.txt"
    val globPattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "**"

    override def beforeAll(configMap: ConfigMap) {
        startSparkContext()
    }

    override def afterAll(configMap: ConfigMap) {
        stopSparkContext()
    }

    test("test to return general file statistics") {
        val rdd = sc.fileStats(Array(textFilePattern), false, 2)
        val stats = rdd.collect()
        // check number of stats objects
        stats.length should be (2)
        // check that size > 0 and file path contains "org/sparkpipe/rdd"
        stats.foreach(entry => {
            assert(entry.size > 0)
            assert(entry.path.contains("org" + / + "sparkpipe" + / + "rdd"))
            assert(entry.checksum == null)
        })
    }

    test("statistics should fail when pattern is empty") {
        intercept[Exception] {
            val rdd = sc.fileStats("", false, 2)
            rdd.count()
        }
    }

    test("statistics should generate checksum") {
        val rdd = sc.fileStats(textFilePattern, true, 2)
        val stats = rdd.collect()
        // check number of stats objects
        stats.length should be (2)
        // check that size > 0 and file path contains "org/sparkpipe/rdd"
        stats.foreach(entry => {
            assert(entry.checksum != null && entry.checksum.length() > 0)
        })
    }
}
