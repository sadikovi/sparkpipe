package org.sparkpipe.rdd

import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, Dependency, OneToOneDependency}
import org.apache.spark.rdd.RDD
import org.sparkpipe.rdd.util.SerializableConfiguration

/**
 * Abstract class that provides common API for RDDs that work with files, e.g.
 * `FilenameCollectionRDD`, or `FileStatisticsRDD`. Implicitly uses default configuration that will
 * be serialized to send to all workers.
 */
private[rdd] abstract class FileRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]])(
    implicit _conf: Configuration = new Configuration(false)
) extends RDD[T](sc, deps) {
    private val confBroadcast = sparkContext.broadcast(new SerializableConfiguration(_conf))

    def getConf(): Configuration = {
        val conf: Configuration = confBroadcast.value.value
        conf
    }

    def this(@transient oneParent: RDD[_]) =
        this(oneParent.context , List(new OneToOneDependency(oneParent)))
}
