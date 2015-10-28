package org.sparkpipe.rdd

import java.io.{File => JavaFile}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal
import org.apache.spark.{SparkContext, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path, PathFilter, FileUtil}

/**
 * :: Experimental ::
 * RDD that returns filenames for a given file pattern.
 * Uses hadoop fs libraries to handle different file paths (HDFS, local).
 * Be aware that each pattern will result in collection of file paths for the same partition,
 * which means you may want to repartition it afterwards to distirbute data evenly.
 * It is recommended to use `sc.fileName` with `splitPerFile = true`, it will put each resolved
 * file path in its own partition. This works great in combination with PipedRDD.
 * Currently, [[FilenameCollectionRDD]] supports global pattern resolution "globstar" for local
 * file system as well as HDFS.
 * Usage:
 * Having directory structure like this:
 * data
 * +- master
 *    +- logs
 *       +- log1.gz
 *    +- temp
 *       +- log2.gz
 * +- slave
 *    +- slave-log.gz
 * {{{
 * val filePattern = "/ data / * / * / *.gz"
 * val rdd = sc.fileName(filePattern)
 * // this will return
 * // /data/master/logs/log1.gz
 * // /data/master/temp/log2.gz
 *
 * // though using globstar...
 * val filePattern = "/ data / ** / *.gz"
 * val rdd = sc.fileName(filePattern)
 * // it will return
 * // /data/master/logs/log1.gz
 * // /data/master/temp/log2.gz
 * // /data/slave/slave-log.gz
 * }}}
 */

/** PathFilter that converts suffix of file pattern into simple regular expression */
private[rdd] class PathSuffixFilter(private val regex: String) extends PathFilter {
    private val expected: Array[String] = regex.split(Path.SEPARATOR).map(x => convertToRegex(x))

    private def convertToRegex(str: String, matchExactly: Boolean = true): String = {
        // escape original [.^$] with \symbol
        val escapedStr = str.replaceAllLiterally(".", "\\.").replaceAllLiterally("^", "\\^").
        replaceAllLiterally("$", "\\$")
        //now replace all occuriences of [*] on [.*]
        val pattern = escapedStr.replaceAllLiterally("*", ".*")
        // return final pattern, close regex if you want to match exactly (recommended)
        if (matchExactly) "^" + pattern + "$" else pattern
    }

    def deny(path: Path): Boolean = !accept(path)

    def accept(path: Path): Boolean = {
        // we match regex and path by each directory
        val got = path.toString().split(Path.SEPARATOR).takeRight(expected.length)
        if (got.length != expected.length) {
            return false
        }
        got.zipWithIndex.forall { case (elem, index) => {
            val opt = expected(index).r.findFirstMatchIn(elem)
            opt.isDefined
        }}
    }
}

/** Filename partition, the use case is String class tag, but `T` is left for some generality. */
private[rdd] class FilenameCollectionPartition[T: ClassTag] (
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]
) extends Partition with Serializable {
    def iterator: Iterator[T] = values.iterator

    override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

    override def equals(other:Any): Boolean = other match {
        case that: FilenameCollectionPartition[_] =>
            this.rddId == that.rddId && this.slice == that.slice
        case _ => false
    }

    override def index: Int = slice
}

/**
 * [[FilenameCollectionRDD]] supports only String as a ClassTag for now, taking a sequence of file
 * patterns as strings. Supports normal parsing mode, and mode with a globstar to search
 * recursively, similar to `find /.../.../ -name -type f`. Globstar should be used once in file
 * pattern, if pattern has more than one occurience of "**", then the first one is considered valid
 * and the rest is a part of suffix string that will be matched literally. FilenameCollectionRDD
 * supports file patterns with globstar for both HDFS and local file systems.
 */
private[rdd] class FilenameCollectionRDD[T<:String: ClassTag] (
    @transient sc: SparkContext,
    @transient data: Seq[T],
    numSlices: Int
) extends RDD[String](sc, Nil) {
    override def getPartitions: Array[Partition] = {
        val slices = this.slice(data, numSlices).toArray
        slices.indices.map(i => new FilenameCollectionPartition[T](id, i, slices(i))).toArray
    }

    override def compute(s: Partition, context: TaskContext): Iterator[String] = {
        val GLOBSTAR = "**"
        val paths = for (elem <- s.asInstanceOf[FilenameCollectionPartition[T]].iterator) yield {
            val path = new Path(elem)
            val fs = path.getFileSystem(new JobConf())
            // resolve path, whether it is local path
            val resolvedPath = if (path.isAbsolute) {
                path
            } else {
                val workDir = fs.getWorkingDirectory()
                workDir.suffix(Path.SEPARATOR + path.toString())
            }
            val uri = resolvedPath.toString()
            logInfo("URI is " + uri)
            // resolve globstar pattern
            if (uri.contains(GLOBSTAR)) {
                // buffer for all globally resolved file paths
                val buffer: ArrayBuffer[String] = ArrayBuffer()
                val arr = uri.split(Path.SEPARATOR)
                val index = arr.indexOf(GLOBSTAR)
                // make sure that path does not start with **, though it can end with **. In this
                // case we return everything that we can find recursively in that directory. If file
                // pattern contains several globstars, we process only the first one, the rest will
                // be part of normal file pattern.
                if (index > 0 && index < arr.length) {
                    logInfo("Found global " + GLOBSTAR + " pattern for element " + elem +
                        ". This will be resolved according to org.apache.hadoop.fs.FileSystem " +
                        "and PathSuffixFilter")
                    // take everything before pattern, search recursively and filter by another part
                    // we use index + 1 to drop globstar itself from suffix. If globstar is the last
                    // item, then suffix should be additional `*`, since we want every match
                    val baseDirPattern = arr.take(index).mkString(Path.SEPARATOR)
                    val suffixPattern = if (index < arr.length - 1)
                        arr.drop(index + 1).mkString(Path.SEPARATOR) else "*"
                    val filter = new PathSuffixFilter(suffixPattern)
                    // resolve base directory as a sequence of Path files
                    val statuses = Option(fs.globStatus(new Path(baseDirPattern)))
                    if (statuses.isDefined) {
                        val dirs = FileUtil.stat2Paths(statuses.get)
                        for (dir <- dirs; if fs.isDirectory(dir)) {
                            val iterOpt = Try(fs.listFiles(dir, true)).toOption
                            if (iterOpt.isDefined) {
                                val iter = iterOpt.get
                                while (iter.hasNext) {
                                    val elem = iter.next
                                    if (elem != null && filter.accept(elem.getPath)) {
                                        buffer.append(elem.getPath.toString())
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // if element contains invalid globstar parameter, we still try parsing it, as
                    // it can be part of an actual file pattern
                    val statuses = Option(fs.globStatus(resolvedPath))
                    if (statuses.isDefined) {
                        for (status <- statuses.get) {
                            buffer.append(status.getPath.toString())
                        }
                    }
                }
                buffer.toArray
            } else {
                val statuses = Option(fs.globStatus(resolvedPath))
                statuses match {
                    case Some(arr) => arr.map(status => status.getPath().toString)
                    case None => Array[String]()
                }
            }
        }
        paths.flatMap(list => list).toIterator
    }

    private def slice(seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
        require(numSlices >= 1, "Positive number of slices required")

        def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
            (0 until numSlices).iterator.map(i => {
                val start = ((i * length) / numSlices).toInt
                val end = (((i + 1) * length) / numSlices).toInt
                (start, end)
            })
        }

        val array = seq.toArray
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
        }.toSeq
    }
}
