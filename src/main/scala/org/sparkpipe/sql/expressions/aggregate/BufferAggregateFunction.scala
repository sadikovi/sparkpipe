package org.sparkpipe.sql.expressions.aggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.sparkpipe.sql.types.{Buffer, BufferType}

/**
 * UDAF for Long values collection (usually timestamps). Also requires tslimit that restricts
 * maximum number of values in collection. If record exceeds number of timestamps comparing to
 * `tslimit`, overall collection is reset to empty. This function aims for better performance
 * compare to [[CollectionFunction]], it uses custom [[BufferType]] to perform merge of values. It
 * allows to overcome problem of Out Of Memory (Java heap) exception for datasets with large
 * cardinality.
 * {{{
 * import org.sparkpipe.sql.expressions.aggregate.BufferAggregationFunction
 * val a = sc.parallelize(0L to 20L).map(x => (x, x % 4)).toDF("value", "group")
 * val cl = new BufferAggregationFunction(5)
 * val df = a.groupBy("group").agg(cl($"value").as("list")).cache()
 * }}}
 */
 class BufferAggregationFunction(
     private val tslimit: Int
 ) extends UserDefinedAggregateFunction {
     def inputSchema: StructType =
         StructType(StructField("value", LongType, false) :: Nil)

     def bufferSchema: StructType =
         StructType(StructField("list", BufferType, true) ::
            StructField("isnull", BooleanType, false) :: Nil)

     override def dataType: DataType = ArrayType(LongType, true)

     def deterministic: Boolean = true

     def initialize(buffer: MutableAggregationBuffer): Unit = {
         buffer(0) = new Buffer()
         buffer(1) = false
     }

     def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
         val isNull = buffer(1).asInstanceOf[Boolean]
         if (!isNull && !input.isNullAt(0)) {
             var buf = buffer(0).asInstanceOf[Buffer]
             if (buf.length < tslimit) {
                 buf.append(input.getAs[Long](0))
             } else {
                 buffer(1) = true
                 buf.clear()
             }
             buffer(0) = buf
             buf = null
         }
     }

     def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
         val isNull = buffer1(1).asInstanceOf[Boolean]
         if (!isNull && !buffer2.isNullAt(0)) {
             var buf1 = buffer1(0).asInstanceOf[Buffer]
             var buf2 = buffer2(0).asInstanceOf[Buffer]
             if (buf1.length + buf2.length <= tslimit) {
                 buf1.appendAll(buf2)
             } else {
                 buffer1(1) = true
                 buf1.clear()
                 buf2.clear()
             }
             buffer1(0) = buf1
             buf1 = null
             buf2 = null
         }
     }

     // we still return ArrayType instead of BufferType, so we do not need to perform conversion
     def evaluate(buffer: Row): Any = {
         val buf = buffer(0).asInstanceOf[Buffer]
         buf.toSeq
    }
 }
