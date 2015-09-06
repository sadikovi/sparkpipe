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
}
