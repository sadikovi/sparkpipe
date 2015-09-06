package org.sparkpipe.util.io

import java.io.File
import org.sparkpipe.test.util.UnitTestSpec

class PathsSpec extends UnitTestSpec {
    /** tests local FS operations with Paths */
    test("Paths should create local path") {
        val paths = Seq(
            ("/home/temp/*.txt", true),
            ("home/temp/*.txt", true),
            ("file://home/temp/*.txt", true),
            ("file:///home/temp/*.txt", true),
            ("file:///home/temp/*.txt/", true),
            ("home/ano:ther/*.csv", false),
            ("file:/home/temp", false),
            ("//home/temp", false),
            ("home/{} path", false),
            ("file://home/file://test", false)
        )

        paths.foreach(path => path match {
            case (a: String, true) =>
                val dir = new LocalPath(a)
                dir.local should equal (a.stripPrefix("file://").stripSuffix("/"))
            case (b: String, false) =>
                intercept[IllegalArgumentException] {
                    new LocalPath(b)
                }
            case _ =>
                throw new Exception("Wrong argument for test")
        })
    }

    /** tests HDFS paths */
    test("Paths should work for HDFS paths") {
        val paths = Seq(
            ("hdfs://host:50700/home/temp/*.csv", true),
            ("hdfs://anotherhost:80/home/temp/*.csv", true),
            ("hdfs://anotherhost:80/home/temp/*.csv/", true),
            ("hdfs://anotherhost:9/home/temp/*.csv", false),
            ("hdfs:/anotherhost:50700/home/temp/*.csv", false),
            ("file://host:50700/home/temp/*.csv", false),
            ("/home/temp/*.csv", false)
        )

        paths.foreach(path => path match {
            case (a:String, true) =>
                val dir = new HadoopPath(a)
                dir.uri should equal (a.stripSuffix("/"))
            case (b:String, false) =>
                intercept[IllegalArgumentException] {
                    new HadoopPath(b)
                }
            case _ =>
                throw new Exception("Wrong argument for test")
        })
    }

    /** simple test to correctly recognize FS */
    test("Paths should recognize FS correctly") {
        val paths = Seq(
            ("/home/temp/*.txt", true),
            ("home/temp/*.txt", true),
            ("file://home/temp/*.txt", true),
            ("hdfs://host:50700/home/temp/*.csv", false)
        )

        paths.foreach(path => {
            val dir = Paths.fromString(path._1)
            dir.isLocalFS should be (path._2)
        })
    }

    test("Paths should return correct absolute path") {
        val paths = Seq(
            ("/home/temp/*.txt", "/home/temp/*.txt"),
            ("home/temp/*.txt", new File("home/temp/*.txt").getAbsolutePath),
            ("file://home/temp/*.txt", new File("home/temp/*.txt").getAbsolutePath),
            ("file:///home/temp/*.txt", "/home/temp/*.txt"),
            ("file:///home/temp/*.txt/", "/home/temp/*.txt"),
            ("hdfs://host:50700/home/temp/*.csv", "/home/temp/*.csv"),
            ("hdfs://host:50700/home/temp/*.csv/", "/home/temp/*.csv")
        )

        paths.foreach(path => {
            Paths.fromString(path._1).absolute should equal (path._2)
        })
    }

    test("Paths should return correct local path") {
        val paths = Seq(
            ("/home/temp/*.txt", "/home/temp/*.txt"),
            ("home/temp/*.txt", "home/temp/*.txt"),
            ("file://home/temp/*.txt", "home/temp/*.txt"),
            ("file:///home/temp/*.txt", "/home/temp/*.txt"),
            ("file:///home/temp/*.txt/", "/home/temp/*.txt"),
            ("hdfs://host:50700/home/temp/*.csv", "/home/temp/*.csv"),
            ("hdfs://host:50700/home/temp/*.csv/", "/home/temp/*.csv")
        )

        paths.foreach(path => {
            Paths.fromString(path._1).local should equal (path._2)
        })
    }

    test("Paths should return correct URI") {
        val paths = Seq(
            ("/home/temp/*.txt", "file:///home/temp/*.txt"),
            ("home/temp/*.txt", "file://" + new File("home/temp/*.txt").getAbsolutePath),
            ("file://home/temp/*.txt", "file://" + new File("home/temp/*.txt").getAbsolutePath),
            ("file:///home/temp/*.txt", "file:///home/temp/*.txt"),
            ("file:///home/temp/*.txt/", "file:///home/temp/*.txt"),
            ("hdfs://host:50700/home/temp/*.csv", "hdfs://host:50700/home/temp/*.csv"),
            ("hdfs://host:50700/home/temp/*.csv/", "hdfs://host:50700/home/temp/*.csv")
        )

        paths.foreach(path => {
            Paths.fromString(path._1).uri should equal (path._2)
        })
    }

    /** tests root as host:port for HDFS */
    test("Paths should return correct root URL for HDFS") {
        val paths = Seq(
            ("hdfs://host:50700/home/temp/*.csv", "hdfs://host:50700"),
            ("hdfs://anotherhost:8080/home/temp/*.csv", "hdfs://anotherhost:8080"),
            ("hdfs://another-host:80/home/temp/*.csv/", "hdfs://another-host:80"),
            ("hdfs://host1:9190/home/temp/*.csv", "hdfs://host1:9190")
        )

        paths.foreach(path => {
            Paths.fromString(path._1).root should equal (path._2)
        })
    }
}
