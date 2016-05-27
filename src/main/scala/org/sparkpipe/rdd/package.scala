/**
 * Implicts for RDDs.
 * import them as `import org.sparkpipe.rdd.implicits._`
 * structure of implicits package as follows:
 * main package (only one): package org.sparkpipe.rdd
 *  object implicits
 *   implicit classes and functions
 */
package org.sparkpipe.rdd

package object implicits {
    import scala.io.Codec
    import scala.reflect.ClassTag
    import org.apache.hadoop.io.{LongWritable, Text}
    import org.apache.hadoop.mapred.{TextInputFormat, FileSplit}
    import org.apache.spark.SparkContext
    import org.apache.spark.rdd.{RDD, HadoopRDD}

    /**
     * :: Experimental ::
     * RichSparkContextFunctions class provides some additional functionality to RDDs.
     * All methods can be called on SparkContext similar to standard methods,
     * such as `sc.textFile`.
     */
    implicit class RichSparkContextFunctions(sc: SparkContext) {
        // Maximum number of partitions when using `splitPerFile` option
        val MAX_NUM_PARTITIONS: Int = 65536

        /**
         * Resolves local or HDFS file patterns and returns list with file paths.
         * `numSlices` is a number of partitions to use for file patterns, not results.
         * `splitPerFile` allows to store each resolved file path to be in its own partition. Very
         * useful, if using with PipedRDD chains.
         */
        def fileName(
            files: Array[String],
            numSlices: Int,
            splitPerFile: Boolean = true
        ): RDD[String] = {
            val rdd = new FilenameCollectionRDD[String](sc, files, numSlices)
            if (!splitPerFile) {
                return rdd
            }
            // Cache RDD and repartition by number of files. We cannot exceed maximum number of
            // partitions, after which splitPerFile will not work
            val numFiles = rdd.cache().count.toInt
            if (numFiles > MAX_NUM_PARTITIONS) {
                rdd.repartition(MAX_NUM_PARTITIONS)
            } else if (numFiles > 0) {
                rdd.repartition(numFiles)
            } else {
                rdd
            }
        }

        /** Filename RDD with default number of partitions */
        def fileName(files: Array[String]): RDD[String] = {
            fileName(files, sc.defaultMinPartitions, true)
        }

        /** Convinience method to specify paths as arguments */
        def fileName(files: String*): RDD[String] =
            fileName(files.toArray)

        /**
         * File statistics for a file pattern/s.
         * Uses FilenameCollectionRDD to resolve patterns, allows to repartition files afterwards.
         * Returns RDD of [[FileStatistics]] entries.
         */
        def fileStats(
            files: Array[String],
            withChecksum: Boolean,
            numPartitions: Int
        ): RDD[FileStatistics] = {
            // process and resolve files
            val numFiles = files.length
            val splitPerFile = numPartitions <= 0
            val filesCollection = fileName(files, numFiles, splitPerFile)
            // if number of partitions specified, we do not split per file, as we can repartition
            // it once afterwards
            val rdd = if (numPartitions > 0) {
                filesCollection.repartition(numPartitions)
            } else {
                filesCollection
            }
            new FileStatisticsRDD(rdd, withChecksum)
        }

        /** Convinience method for file statistics since usually only one pattern is passed */
        def fileStats(
            pattern: String,
            withChecksum: Boolean,
            numPartitions: Int
        ): RDD[FileStatistics] =
            fileStats(Array[String](pattern), withChecksum, numPartitions)

        /** File statistics with maximum number of partitions, each file per partition */
        def fileStats(files: Array[String], withChecksum: Boolean): RDD[FileStatistics] =
            fileStats(files, withChecksum, numPartitions = 0)

        /** File statistics with maximum number of partitions, each file per partition */
        def fileStats(files: String*): RDD[FileStatistics] =
            fileStats(files.toArray, withChecksum = true, numPartitions = 0)

        /** Return RDD of pairs of filename, offset in bytes, and content line */
        def textContent(
            path: String,
            minPartitions: Int = sc.defaultMinPartitions
        ): RDD[(String, Long, String)] = {
            val rdd = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable],
                classOf[Text], minPartitions).asInstanceOf[HadoopRDD[LongWritable, Text]]

            val hadoopRDD = rdd.mapPartitionsWithInputSplit((split, iter) => {
                val path = split.asInstanceOf[FileSplit].getPath().toString()

                new Iterator[(String, Long, String)] {
                    def next(): (String, Long, String) = {
                        val pair = iter.next()
                        val bytes: Long = pair._1.get()
                        val content: String = pair._2.toString
                        // return path to the file split and content for the line
                        (path, bytes, content)
                    }

                    def hasNext: Boolean = {
                        iter.hasNext
                    }
                }
            }, preservesPartitioning = true)
            hadoopRDD
        }
    }

    /**
     * :: Experimental ::
     * RichRDDFunctions class provides more functionality to RDDs inherited from
     * `spark.rdd.RDD`. Usually, it is for RDDs as narrow dependencies with defined parents.
     * For no-parent RDDs, look up `RichSparkContext`.
     */
    implicit class RichRDDFunctions[T: ClassTag](rdd: RDD[T]) {
        /** pipes parent RDD with encoding specified */
        def pipeWithEncoding(encoding: String, strict: Boolean): EncodePipedRDD[T] =
            new EncodePipedRDD[T](rdd, "bash", encoding, strict)

        /** pipes parent RDD with codec specified */
        def pipeWithEncoding(strict: Boolean): EncodePipedRDD[T] =
            pipeWithEncoding(Codec.UTF8.toString, strict)

        /** pipes parent RDD with default UTF-8 encoding */
        def pipeWithEncoding(): EncodePipedRDD[T] =
            pipeWithEncoding(Codec.UTF8.toString, true)
    }
}
