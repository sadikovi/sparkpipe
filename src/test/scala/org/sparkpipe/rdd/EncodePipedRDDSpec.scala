package org.sparkpipe.rdd

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

        intercept[Exception] {
            a.pipeWithEncoding("UTF-8", strict=true).count
        }
    }

    /** test is only for ASCII encoding */
    test("test of malformed input failure during pipe") {
        val testFile = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" +
            / + "sample.txt"
        val a = sc.parallelize(Array(testFile)).map("cat " + _)
        val b = a.pipeWithEncoding("ASCII", strict=false)
        b.collect.length should be (Source.fromFile(testFile).getLines().length)

        intercept[Exception] {
            a.pipeWithEncoding("ASCII", strict=true).count
        }
    }

    /** test of complex / piped commands */
    test("complex commands test") {
        val testFile = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" +
            / + "sample.txt"
        val a = sc.parallelize(Array(testFile)).
            map("cat " + _ + " | " + "grep -i \"as\"" + " | " + "perl -ne 'print $_'")
        val b = a.pipeWithEncoding("UTF-8", strict=false)
        val lines = b.collect
        lines.length should be (3)
        lines.sortWith(_ < _) should be (Array(
            "Also translated as logical aggregates or associative compounds, these characters have been",
            "Compound ideograms[edit] | sample bash check",
            "interpreted as combining two or more pictographic or ideographic characters to suggest a third"
        ))
    }

    /** test of log sample */
    test("test of a log sample") {
        val testFile = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" +
            / + "sample-log.txt"
        val a = sc.parallelize(Array(testFile)).
            map("cat " + _ + " | " + "grep DENIED" + " | " + "perl -ne 'print $_'")
        val b = a.pipeWithEncoding("UTF-8", strict=false)
        val lines = b.collect
        lines.length should be (2)
        lines.foreach(
            line => line.contains("10.97.216.133")
        )
    }

    test("empty commands test") {
        val a = sc.parallelize(Array("", ""))
        val b = a.pipeWithEncoding()
        intercept[Exception] {
            b.count
        }
    }

    /** Issue #11 - EncodePipedRDD redirect bug */
    test("redirects to /dev/null or 2>&1") {
        val testFile = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" +
            / + "sample.txt"
        val expected = Source.fromFile(testFile).getLines().toArray
        // testing 2>&1
        val a = sc.parallelize(Array(testFile)).
            map("cat " + _ + " 2>&1 | perl -ne 'print $_'").
            pipeWithEncoding()
        a.collect() should be (expected)
        // testing /dev/null
        val b = sc.parallelize(Array(testFile)).
            map("cat " + _ + " 2>/dev/null | perl -ne 'print $_'").
            pipeWithEncoding()
        b.collect() should be (expected)
        // testing cases where it should not drop them
        val c = sc.parallelize(Array(testFile)).
            map("cat " + _ + " 2 > &1 | perl -ne 'print $_'").
            pipeWithEncoding("UTF-8", strict = false)
        val rows = c.filter(_.startsWith("[ERROR]"))collect()
        rows.length should be (3)
        // testing escaped cases
        val d = sc.parallelize(Array(testFile)).
            map("cat " + _ + " 2>&1 | perl -ne 'print \"2>&1\".$_'").
            pipeWithEncoding()
        d.collect().forall(record => record.contains("2>&1")) should be (true)
    }
}
