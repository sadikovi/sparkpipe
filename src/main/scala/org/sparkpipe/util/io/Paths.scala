package org.sparkpipe.util.io

import java.io.File


/** Patterns for file path [Linux-like systems only] */
private[io] trait Pattern {
    // directory segment pattern
    protected val DIR = """[^:;\/\\\{\}]+"""
    // local directory pattern (does not match the whole string)
    protected val LOCAL_DIR = "(" + DIR + ")" + "(" + """\/""" + DIR + ")*"
    // absolute directory pattern (does not match the whole string)
    protected val ABSOLUTE_DIR = "(" + """\/""" + DIR + ")+"

    /**
     * Pattern for local directory.
     * Matches entire string.
     * @return pattern as string
     */
    def localPattern: String = "^" + LOCAL_DIR + "$"


    /**
     * Pattern for absolute directory.
     * Matches entire string.
     * @return pattern as string
     */
    def absolutePattern: String = "^" + ABSOLUTE_DIR + "$"


    /**
     * Local URI pattern. URI + local directory.
     * @return pattern as string
     */
    def localUriPattern: String


    /**
     * Absolute URI pattern. URI + absolute directory.
     * @return pattern as string
     */
    def absoluteUriPattern: String
}

/** Path class to provide general interface */
abstract class Path extends Serializable {

    /**
     * Tests file system.
     * @return true, if file system is local
     */
    def isLocalFS: Boolean


    /**
     * Local version of path.
     * @return local path
     */
    def local: String


    /**
     * Absolute version of path.
     * @return absolute path
     */
    def absolute: String


    /**
     * URI for a path.
     * @return path as URI string
     */
    def uri: String

    /**
     * Returns root url.
     * @return path as URL
     */
    def root: String
}

/** Local path for Linux-like systems */
class LocalPath(private val filepath: String) extends Path with Pattern {
    // URI prefix for local path
    private val UriPrefix = "file://"

    // cleaned path
    private val path = filepath.stripSuffix("/") match {
        case a if localPattern.r.findFirstMatchIn(a).nonEmpty =>
            localPattern.r.findFirstMatchIn(a).get.group(0)

        case b if absolutePattern.r.findFirstMatchIn(b).nonEmpty =>
            absolutePattern.r.findFirstMatchIn(b).get.group(0)

        case c if localUriPattern.r.findFirstMatchIn(c).nonEmpty =>
            localUriPattern.r.findFirstMatchIn(c).get.group(1)

        case d if absoluteUriPattern.r.findFirstMatchIn(d).nonEmpty =>
            absoluteUriPattern.r.findFirstMatchIn(d).get.group(1)

        case _ => throw new IllegalArgumentException(s"Path $filepath is not a FS path")
    }

    // file for the path to use some features of `java.io.File`
    private val file = new File(path)

    /**
     * Local URI pattern.
     * @return pattern as string
     */
    def localUriPattern: String =
        "^" + """file:\/\/""" + "(" + this.LOCAL_DIR + ")" + "$"

    /**
     * Absolute URI pattern.
     * @return pattern as string
     */
    def absoluteUriPattern: String =
        "^" + """file:\/\/""" + "(" + this.ABSOLUTE_DIR + ")" + "$"

    /**
     * Returns true as it is a local path.
     * @return true, if file system is local
     */
    def isLocalFS: Boolean = true

    /**
     * Absolute path. Returns itself, if path is already absolute.
     * @return absolute path
     */
    def absolute: String = file.getAbsolutePath

    /**
     * Returns original path.
     * @return path
     */
    def local: String = path

    /**
     * Returns URI as string in format "file://[filepath]"
     * @return URI as string for a path
     */
    def uri: String = UriPrefix + this.absolute

    /**
     * Returns uri prefix and root directory. Not recommended to use.
     * @return root URL
     */
    def root: String = UriPrefix + "/"

    override def toString: String = "@LocalPath: " + this.local
}

/** Path for HDFS */
class HadoopPath(private val filepath: String) extends Path with Pattern {
    // HDFS prefix
    val UriPrefix = "hdfs://"

    // host and port to identify HDFS and absolute file path
    private val (host, port, path) = filepath.stripSuffix("/") match {
        case a if absoluteUriPattern.r.findFirstMatchIn(a).nonEmpty =>
            val matched = absoluteUriPattern.r.findFirstMatchIn(a).get
            (matched.group(1), matched.group(2), matched.group(3))

        case _ => throw new IllegalArgumentException(s"Path $filepath is not Hadoop FS path")
    }

    /** Local URI pattern. Not supported for HDFS. */
    def localUriPattern: String =
        throw new UnsupportedOperationException("Local URI is not supported")

    /**
     * Absolute URI pattern. Parses host, port and filepath.
     * @return pattern as string
     */
    def absoluteUriPattern: String =
        "^" + """hdfs:\/\/""" + """([\w\.-]+)""" + ":" + """([\d]{2,8})""" +
        "(" + this.ABSOLUTE_DIR + ")" + "$"

    /**
     * Tests whether file system is local. For Hadoop Path returns false.
     * @return true, if file system is local
     */
    def isLocalFS: Boolean = false

    /**
     * Equals absolute path in case of HDFS.
     * @return local filepath
     */
    def local: String = this.absolute

    /**
     * Returns path component of URI.
     * @return path
     */
    def absolute: String = path

    /**
     * Returns URI for HDFS.
     * @return URI as string
     */
    def uri: String = UriPrefix + host + ":" + port + this.absolute

    /**
     * Returns path root as hdfs://host:port.
     * @return root as URL
     */
    def root: String = UriPrefix + host + ":" + port

    override def toString: String = "@HadoopPath: " + this.local
}

/** Factory to create paths */
object Paths {

    /**
     * Returns instance of a Path. Use `isLocalFS` method to identify locality.
     *
     * @param filepath path to parse
     * @return Path instance
     */
    def fromString(filepath: String): Path =
        filepath match {
            case a if a.startsWith("hdfs://") => new HadoopPath(filepath)
            case _ => new LocalPath(filepath)
        }
}
