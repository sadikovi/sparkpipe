package org.sparkpipe.rdd

import java.io.{FileInputStream, File => JavaFile}
import java.nio.file.Files
import java.nio.file.attribute.{BasicFileAttributes, FileTime}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.sparkpipe.util.io.os

/**
 * :: Experimental ::
 * FileStatistics object keeps information of possible statistics we can collect from a file.
 * `path` - absolute file path as uri
 * `file` - name of the file
 * `size` - size in bytes
 * `permissions` - set of permissions, applicable only on Linux
 * `datecreated` - date when file is created, if applicable
 * `datemodified` - date when file is modified, if applicable
 * `checksum` - checksum using MD5 hash for local file system, and MD5-of-0MD5-of-512CRC32C for
 * HDFS, other file systems are not supported (will returned empty string). Consider moving from
 * generating MD5 sum to using CRC32, if working with very large files.
 */
private[rdd] case class FileStatistics(
    val path: String,
    val file: String,
    val size: Long,
    val permissions: String,
    val datecreated: Long,
    val datemodified: Long,
    val checksum: String
)

/**
 * :: Experimental ::
 * `FileStatisticsRDD` returns statistics for each entry assuming that entry is a valid file path,
 * either HDFS or local file system, otherwise it will throw an exception. Statistics include
 * full file path, name, size, date modified and etc.
 * Be default, does not generate checksum for files, since it is relatively expensive. Checksum
 * field in this case will be empty.
 */
private[rdd] class FileStatisticsRDD[T: ClassTag](
    prev: RDD[T],
    private val withChecksum: Boolean = false
) extends FileRDD[FileStatistics](prev) {
    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[FileStatistics] = {
        // we assume that every entry is a valid file path. If entry does not comply to being
        // file path, we throw an exception, without trying to process the rest.
        val conf = getConf()
        // buffer for statistics objects
        val buff: ArrayBuffer[FileStatistics] = new ArrayBuffer()
        // compute stats for each entry
        for (elem <- firstParent[T].iterator(split, context)) {
            val path = new Path(elem.toString())
            val fs = path.getFileSystem(conf)
            // resolve path relatively to current directory
            val resolvedPath = fs.resolvePath(path)
            val statuses = Option(fs.globStatus(resolvedPath))
            if (statuses.isEmpty) {
                throw new IllegalArgumentException("Cannot resolver status for a file path: " +
                    resolvedPath)
            }
            val statusArr = statuses.get
            if (statusArr.length > 1) {
                logWarning("Found more than one status for a file path \"" + resolvedPath +
                    "\". Will use the first status")
            }
            val status = statusArr(0)
            val updatedPath = status.getPath()
            val updatedFile = new JavaFile(updatedPath.toString().stripPrefix("file:"))
            val updatedScheme = fs.getScheme()
            // collect statistics from FileStatus instance
            // - URI of the path
            val pathStr = updatedPath.toString()
            // - name of the file
            val name = updatedPath.getName()
            // - size in bytes
            val size: Long = status.getLen()
            // - permissions, as string representation of `Permission` object
            val permission: String = status.getPermission().toString()
            // - modified date
            val dateModified: Long = status.getModificationTime()
            // - creation date, works only for local Linux / OS X file system, otherwise return -1
            val dateCreated: Long = if (updatedScheme == "file" && (os.isUnix() || os.isMac())) {
                val attrs = Files.readAttributes(updatedFile.toPath, "creationTime")
                // attribute "creationTime" is an instance of `java.nio.file.attribute.FileTime`
                attrs.get("creationTime").asInstanceOf[FileTime].toMillis()
            } else {
                logWarning("Created date for a file currently only supports local file system " +
                    "either Linux or OS X")
                -1L
            }
            // - checksum, only local file system and HDFS support checksum generation for now.
            // md5hex for local, and MD5-of-0MD5-of-512CRC32C for HDFS (default)
            val checksum = if (!withChecksum) {
                logDebug("Checksum usage is off for path " + updatedPath.toString())
                null
            } else {
                if (updatedScheme == "file") {
                    var stream: FileInputStream = null
                    try {
                        stream = new FileInputStream(updatedFile)
                        DigestUtils.md5Hex(stream)
                    } finally {
                        if (stream != null) {
                            stream.close()
                        }
                    }
                } else if (updatedScheme == "hdfs") {
                    val sumWithAlg = fs.getFileChecksum(updatedPath).toString()
                    // split `sumWithAlg` string into name of algorithm and checksum, if length is
                    // less than 2, then we just return original string, otherwise second part,
                    // since it contains actual checksum
                    val parts = sumWithAlg.split(":", 2)
                    if (parts.length == 2) {
                        parts(1)
                    } else {
                        sumWithAlg
                    }
                } else {
                    logWarning("Checksum is not supported for file scheme " + updatedScheme)
                    null
                }
            }

            // add newly created statistics
            buff.append(FileStatistics(
                pathStr, name, size, permission, dateCreated, dateModified, checksum))
        }
        // return iterator of file statistics
        buff.toIterator
    }
}
