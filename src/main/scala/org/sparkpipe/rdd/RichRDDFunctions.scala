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
    import org.apache.spark.rdd.RDD

    /**
     * :: Experimental ::
     * RichRDDFunctions class provides more functionality to RDDs inherited from
     * `spark.rdd.RDD`. Usually, it is for RDDs as narrow dependencies with defined parents.
     * For no-parent RDDs, look up `RichSparkContext`.
     */
    implicit class RichRDDFunctions[T: ClassTag](rdd: RDD[T]) {
        /** pipes parent RDD with encoding specified */
        def pipeWithEncoding(encoding: String, strict: Boolean) =
            new EncodePipedRDD[T](rdd, encoding, strict)

        /** pipes parent RDD with codec specified */
        def pipeWithEncoding(codec: Codec, strict: Boolean) =
            new EncodePipedRDD[T](rdd, codec, strict)

        /** pipes parent RDD with default UTF-8 encoding */
        def pipeWithEncoding() = new EncodePipedRDD[T](rdd, Codec.UTF8, strict=true)
    }
}
