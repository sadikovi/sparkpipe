package org.sparkpipe.rdd

import java.io.IOException
import scala.io.Source
import org.scalatest._
import org.sparkpipe.rdd.implicits._
import org.sparkpipe.test.spark.SparkLocal
import org.sparkpipe.test.util.UnitTestSpec

class EncodePipedRDDSpec extends UnitTestSpec with SparkLocal with BeforeAndAfterAll {
    override def beforeAll(configMap: ConfigMap) {
        startSparkContext()
    }

    override def afterAll(configMap: ConfigMap) {
        stopSparkContext()
    }

    test("RDD should inherit number of partitions") {
        val numPartitions = 10
        val rdd = sc.parallelize(0 to 100, numPartitions)
        val piped = rdd.pipeWithEncoding()
        piped.partitions.length should be (numPartitions)
    }

    /** test of cmd failure, though still reports error in console */
    test("test of command failures during pipe") {
        val a = sc.parallelize(
            Array(
                baseDirectory + / + ".gitignore",
                baseDirectory + / + "NOEXIST.md",
                baseDirectory + / + "sbt-build.sh"
            )
        ).map("cat " + _)
        val c = a.pipeWithEncoding("UTF-8", strict=false)
        c.collect.filter(_.startsWith("[ERROR]")).length should be (1)

        intercept[IOException] {
            a.pipeWithEncoding("UTF-8", strict=true).count
        }
    }

    /** test is only for ASCII encoding */
    test("test of malformed input failure during pipe") {
        val testFile = baseDirectory + / + "temp" + / + "sample.log"
        val a = sc.parallelize(Array(testFile)).map("cat " + _)
        val b = a.pipeWithEncoding("ASCII", strict=false)
        b.collect.length should be (Source.fromFile(testFile).getLines().length)

        intercept[IOException] {
            a.pipeWithEncoding("ASCII", strict=true).count
        }
    }
}
