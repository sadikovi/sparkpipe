package org.sparkpipe.rdd

import org.apache.spark.SparkException
import org.scalatest._
import org.sparkpipe.rdd.implicits._
import org.sparkpipe.test.spark.SparkLocal
import org.sparkpipe.test.util.UnitTestSpec

class FilenameCollectionRDDSpec extends UnitTestSpec with SparkLocal with BeforeAndAfterAll {
    val textFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
        "rdd" + / + "*.txt"
    val configFilePattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / +
        "util" + / + "config" + / + "*.conf"
    val globPattern = testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "**"

    override def beforeAll(configMap: ConfigMap) {
        startSparkContext()
    }

    override def afterAll(configMap: ConfigMap) {
        stopSparkContext()
    }

    test("test to return associated files for local file pattern") {
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
        intercept[Exception] {
            val rdd = sc.fileName("")
            rdd.collect()
        }
    }

    // testing of non-absolute path, rdd should convert local path into absolute by default
    test("test of non-absolute file path") {
        val rdd = sc.fileName("test/local/file.path")
        // should return empty sequence without failing
        rdd.collect().isEmpty should be (true)
    }

    // issue #8 test
    test("issue #8 - test splitPerFile option") {
        val rdd = sc.fileName(Array(textFilePattern), 4, false)
        rdd.partitions.length should be (4)
        val splitRdd = sc.fileName(Array(textFilePattern), 4, true)
        splitRdd.partitions.length should be (2)
    }

    // testing simple globstar functionality in local file system
    test("test of globstar functionality") {
        val rdd = sc.fileName(Array(textFilePattern, configFilePattern))
        val files = rdd.collect()
        // using globstar to fetch those files
        val globRdd = sc.fileName(testDirectory + / + "resources" + / + "org" + / + "sparkpipe" +
            / + "**" + / + "*.(txt|conf)"
        )
        val globFiles = globRdd.collect()
        globFiles.length should equal (files.length)
        globFiles.sortWith(_ < _) should equal (files.sortWith(_ < _))
    }

    // testing file paths when file pattern has globstar at end of pattern
    test("issue #10 - globstar at the end of a pattern") {
        val rdd = sc.fileName(Array(globPattern))
        val files = rdd.map(_.stripPrefix("file:")).collect()

        files.sortWith(_ < _) should equal (
            Array(
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" + / +
                    "sample-log.txt",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "rdd" + / +
                    "sample.txt",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "util" + / +
                    "config" + / + "badproperties.conf",
                testDirectory + / + "resources" + / + "org" + / + "sparkpipe" + / + "util" + / +
                    "config" + / + "properties.conf"
            ).sortWith(_ < _)
        )
    }
}
