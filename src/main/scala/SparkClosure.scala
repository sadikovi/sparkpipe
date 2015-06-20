package org.spark.closure

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object Settings {
    var abort = false

    def init {
        abort = true
    }
}

object Main {
    def main(args:Array[String]) {
        Settings.init

        this run
    }

    def run {
        val appname = "spark-closure-test"

        // spark setup
        val conf = new SparkConf().setAppName(appname)
        val sc = new SparkContext(conf)
        sc.addFile("resource/index.txt")

        // spark workflow
        val abort = Settings.abort
        val loaded = sc.textFile("resource/index.txt").cache
        val filtered = loaded.filter(x => !Settings.abort || x.toLowerCase.startsWith("spark"))
        filtered.coalesce(1, true).saveAsTextFile("out")

        // stop spark context and remove reference
        sc.stop()
    }
}
