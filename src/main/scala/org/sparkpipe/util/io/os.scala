package org.sparkpipe.util.io

package object os {
    private val system = System.getProperty("os.name").toLowerCase()

    def isWindows(): Boolean = {
        system.indexOf("win") >= 0
    }

    def isUnix(): Boolean = {
        system.indexOf("nix") >= 0 || system.indexOf("nux") >= 0 || system.indexOf("aix") >= 0
    }

    def isMac(): Boolean = {
        system.indexOf("mac") >= 0
    }
}
