package org.sparkpipe.rdd

import org.scalatest._
import org.sparkpipe.rdd.implicits._
import org.sparkpipe.test.spark.SparkLocal
import org.sparkpipe.test.util.UnitTestSpec

class TextContentSpec extends UnitTestSpec with SparkLocal with BeforeAndAfterAll {
    val textFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
        "rdd" + / + "*.txt"

    override def beforeAll(configMap: ConfigMap) {
        startSparkContext()
    }

    override def afterAll(configMap: ConfigMap) {
        stopSparkContext()
    }

    test("read files and return pair of filename and content") {
        val rdd = sc.textContent(textFilePattern)
        // collect filenames
        val separator = /.charAt(0)
        val arr = rdd.map { case(file, bytes, content) => {
            file.split(separator).last
        } }.collect()

        arr.distinct.sortWith(_ < _) should be (Array(
            "sample-log.txt",
            "sample.txt"
        ).sortWith(_ < _))
        // check number of records
        arr.length should be (12)
    }

    ignore("check number of partitions") {
        val rdd = sc.textContent(textFilePattern, 8)
        rdd.partitions.length should be (8)
    }
}
