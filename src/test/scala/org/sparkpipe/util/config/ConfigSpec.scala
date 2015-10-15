package org.sparkpipe.util.config

import org.sparkpipe.test.util.UnitTestSpec

class ConfigSpec extends UnitTestSpec {
    val good = getClass().getResource("/org/sparkpipe/util/config/properties.conf").getPath()
    val bad = getClass().getResource("/org/sparkpipe/util/config/badproperties.conf").getPath()

    test("Config should load properties.conf") {
        val dir = good
        val conf = Config.fromPath(dir)
        conf.isEmpty should be (false)
    }

    test("Config should check syntax of the file") {
        val goodConf = Config.fromPath(good)
        goodConf.isEmpty should be (false)

        intercept[Exception] {
            val badConf = Config.fromPath(bad)
        }
    }

    test("Config should load properties correctly") {
        val dir = good
        val conf = Config.fromPath(dir)

        conf.getString("key") should be (Some("value"))
        conf.getString("another") should be (Some("another day in Paradise"))
        conf.getBoolean("boolean", "booleans") should be (Some(false))
        conf.getInt("int", "numbers") should be (Some(1))
        conf.getDouble("double", "numbers") should be (Some(23.8))
        conf.getString("some", "wrong-section") should be (Some("angel in dark"))
        conf.getString("param", "GLOBAL") should be (Some("some"))
        conf.getString("goal", "default") should be (Some("be on the Moon"))
    }

    test("Config should read key-space keys properly") {
        val dir = good
        val conf = Config.fromPath(dir)

        conf.getBoolean("spark.shuffle.spill", "key-space") should be (Some(true))
        conf.getBoolean("spark.file.overwrite", "key-space") should be (Some(false))
        conf.getString("spark.driver.extraJavaOptions", "key-space") should be (
            Some("-Dfile.encoding=UTF-8"))
    }

    test("Duplicate mode test") {
        val dir = good
        val overriden = Config.fromPath(dir, DuplicateMode.Override)
        overriden.getString("goal", "another") should be (Some("ha-ha-ha"))

        val ignored = Config.fromPath(dir, DuplicateMode.Ignore)
        ignored.getString("goal", "another") should be (Some("Reach the stars"))

        intercept[Exception] {
            val thrown = Config.fromPath(dir, DuplicateMode.Throw)
        }
    }

    test("Config should replace property if value is not defined") {
        val dir = good
        val conf = Config.fromPath(dir)

        conf.getStringOrElse("key", "default") should be ("value")
        conf.getStringOrElse("another", "default") should be ("another day in Paradise")
        conf.getBooleanOrElse("boolean", "booleans", true) should be (false)
        conf.getIntOrElse("int", "numbers", -1) should be (1)
        conf.getDoubleOrElse("double", "numbers", -1.0) should be (23.8)
        // unknown properties
        conf.getStringOrElse("another-string-property", "-1.0") should be ("-1.0")
        conf.getIntOrElse("another-int-property", -1) should be (-1)
        conf.getLongOrElse("another-long-property", -1L) should be (-1L)
        conf.getDoubleOrElse("another-double-property", -1.0) should be (-1.0)
        // key-space properties
        conf.getIntOrElse("spark.sql.partitions", 100) should be (100)
    }
}
