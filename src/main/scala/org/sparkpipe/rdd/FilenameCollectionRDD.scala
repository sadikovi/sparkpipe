package org.sparkpipe.rdd

import scala.reflect.ClassTag
import org.apache.spark.{SparkContext, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}


/**
 * :: Experimental ::
 * RDD that returns filenames for a given file pattern.
 * Uses hadoop fs libraries to handle different file paths (HDFS, local).
 * Be aware that each pattern will result in collection of file paths for the same partition,
 * which means you may want to repartition it afterwards to distirbute data evenly.
 */

// Filename partition, the use case is String class tag, but `T` is left for some generality.
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

private[rdd] class FilenameCollectionRDD[T<:String: ClassTag] (
    @transient sc: SparkContext,
    @transient data: Seq[T],
    numSlices: Int
) extends RDD[String](sc, Nil) {

    override def getPartitions: Array[Partition] = {
        val slices = this.slice(data, numSlices).toArray
        slices.indices.map(i => new FilenameCollectionPartition[T](id, i, slices(i))).toArray
    }

    override def compute(s:Partition, context:TaskContext): Iterator[String] = {
        val paths = for (elem <- s.asInstanceOf[FilenameCollectionPartition[T]].iterator) yield {
            val path = new Path(elem)
            // TODO: we accept only absolute paths for now, it would be better to make them
            // absolute, if file paths are local.
            require(path.isAbsolute, "Absolute path is required, got " + elem)

            val fs = path.getFileSystem(new JobConf())
            val statuses = fs.globStatus(path)
            // return list of files
            statuses.map(status => status.getPath().toString)
        }
        paths.flatMap(list => list).toIterator
    }

    private def slice(seq:Seq[T], numSlices:Int): Seq[Seq[T]] = {
        require(numSlices >= 1, "Positive number of slices required")

        def positions(length:Long, numSlices:Int): Iterator[(Int, Int)] = {
            (0 until numSlices).iterator.map(i => {
                val start = ((i * length) / numSlices).toInt
                val end = (((i + 1) * length) / numSlices).toInt
                (start, end)
            })
        }

        val array = seq.toArray
        positions(array.length, numSlices).map(
            { case (start, end) => array.slice(start, end).toSeq }
        ).toSeq
    }
}
