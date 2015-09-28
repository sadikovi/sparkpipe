package org.sparkpipe.rdd

import org.scalatest._
import org.sparkpipe.rdd.implicits._
import org.sparkpipe.test.spark.SparkLocal
import org.sparkpipe.test.util.UnitTestSpec

class FilenameCollectionRDDSpec extends UnitTestSpec with SparkLocal with BeforeAndAfterAll {
    override def beforeAll(configMap: ConfigMap) {
        startSparkContext()
    }

    override def afterAll(configMap: ConfigMap) {
        stopSparkContext()
    }

    test("test to return associated files for local file pattern") {
        val textFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
            "rdd" + / + "*.txt"
        val configFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
            "util" + / + "config" + / + "*.conf"

        val rdd = sc.fileName(Array(textFilePattern, configFilePattern), numSlices=2)
        val files = rdd.collect()

        // check that each path is local file system
        files.foreach(_.startsWith("file:") should be (true))

        // check that file paths equal this array, also remove "file:" prefix
        files.map(_.stripPrefix("file:")).sortWith(_ < _) should be (
            Array(
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
                    "rdd" + / + "sample-log.txt",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
                    "rdd" + / + "sample.txt",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
                    "util" + / + "config" + / + "badproperties.conf",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
                    "util" + / + "config" + / + "properties.conf"
            ).sortWith(_ < _)
        )
    }

    test("test of non-existent file pattern") {
        val rdd = sc.fileName("/test/wrong/file/path.*.some")
        val files = rdd.collect()
        files.isEmpty should be (true)
    }

    // test of non-existent file (issue #3)
    test("test of non-existent file path") {
        val rdd = sc.fileName("/test/wrong/file/path.some")
        val files = rdd.collect()
        files.isEmpty should be (true)
    }

    // empty file pattern should throw exception, as HDFS Path class will fail
    test("test of empty file pattern") {
        val rdd = sc.fileName("")
        intercept[Exception] {
            rdd.collect()
        }
    }

    // testing of exception thrown, should catch SparkException instead of general Exception
    test("test of non-absolute file path") {
        val rdd = sc.fileName("test/local/file.path")
        intercept[Exception] {
            rdd.collect()
        }
    }
}
