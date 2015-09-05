package org.sparkpipe.test

import org.sparkpipe.util.UnitTestSpec


class TestSpec extends UnitTestSpec {
    /** prints out paths of the project */
    test("Test spec should report directories") {
        println("base dir: " + baseDirectory())
        println("main dir: " + mainDirectory())
        println("test dir: " + testDirectory())
    }
}
