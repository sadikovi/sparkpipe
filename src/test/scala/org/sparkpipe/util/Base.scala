package org.sparkpipe.util

import java.io.File


private[util] trait Base {
    val RESOLVER = "path-resolver"

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
        val original = getRawPath().split(File.separator)
        require(original.length > 4, "Path length is too short (<= 4): " + original.length)
        val base = original.dropRight(4)
        base.mkString(File.separator)
    }

    /** main directory of the project (./src/main) */
    final protected def mainDirectory(): String = {
        baseDirectory() + File.separator + "src" + File.separator + "main"
    }

    /** test directory of the project (./src/test) */
    final protected def testDirectory(): String = {
        baseDirectory() + File.separator + "src" + File.separator + "test"
    }
}
