package org.sparkpipe.test.util

import java.io.File

private[test] trait Base {
    val RESOLVER = "path-resolver"
    val / = File.separator
    val `:` = File.pathSeparator

    var path: String = ""

    /** returns raw path of the folder where it finds resolver */
    private def getRawPath(): String = {
        if (path.isEmpty) {
            path = getClass().getResource("/" + RESOLVER).getPath()
        }
        path
    }

    /** base directory of the project */
    final protected def baseDirectory(): String = {
        val original = getRawPath().split(/)
        require(original.length > 4, "Path length is too short (<= 4): " + original.length)
        val base = original.dropRight(4)
        base.mkString(/)
    }

    /** main directory of the project (./src/main) */
    final protected def mainDirectory(): String = {
        baseDirectory() + / + "src" + / + "main"
    }

    /** test directory of the project (./src/test) */
    final protected def testDirectory(): String = {
        baseDirectory() + / + "src" + / + "test"
    }

    /** target directory of the project (./target) */
    final protected def targetDirectory(): String = {
        baseDirectory() + / + "target"
    }
}
