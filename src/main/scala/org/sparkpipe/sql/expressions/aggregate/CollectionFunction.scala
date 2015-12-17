package org.sparkpipe.sql.expressions.aggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Collection UDAF for aggregating Long types into a sequence. Uses `tslimit` as an upper bound on
 * a sequence, if length of sequence exceeds limit, returns empty collection.
 * {{{
 * import org.sparkpipe.sql.expressions.aggregate.CollectionFunction
 * val a = sc.parallelize(0 to 20).map(x => (x, x % 4)).toDF("value", "group")
 * val cl = new CollectionFunction(5)
 * val df = a.groupBy("group").agg(cl($"value").as("list")).cache()
 * }}}
 *
 * It still runs relatively slow, so if you are looking for speed up for building collections of
 * `LongType` values, use [[BufferAggregateFunction]] instead.
 */
sealed class CollectionFunction(
    private val limit: Int
) extends UserDefinedAggregateFunction {
    def inputSchema: StructType =
        StructType(StructField("value", LongType, false) :: Nil)

    def bufferSchema: StructType =
        StructType(StructField("list", ArrayType(LongType, true), true) :: Nil)

    override def dataType: DataType = ArrayType(LongType, true)

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = IndexedSeq[Long]()
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (buffer != null) {
            val seq = buffer(0).asInstanceOf[IndexedSeq[Long]]
            if (seq.length < limit) {
                buffer(0) = input.getAs[Long](0) +: seq
            } else {
                buffer(0) = null
            }
        }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if (buffer1(0) != null && buffer2 != null) {
            val seq1 = buffer1(0).asInstanceOf[IndexedSeq[Long]]
            val seq2 = buffer2(0).asInstanceOf[IndexedSeq[Long]]
            if (seq1.length + seq2.length <= limit) {
                buffer1(0) = seq1 ++ seq2
            } else {
                buffer1(0) = null
            }
        }
    }

    def evaluate(buffer: Row): Any = {
        if (buffer(0) == null) {
            IndexedSeq[Long]()
        } else {
            buffer(0).asInstanceOf[IndexedSeq[Long]]
        }
    }
}
