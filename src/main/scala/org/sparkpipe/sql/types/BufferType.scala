package org.sparkpipe.sql.types

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, UserDefinedType}

/**
 * Custom buffer type to speed up execution of collection function. It is not recommended to use it
 * outside of this package. Underlying SQL structure is ArrayType of LongType primitives, java
 * class is an ArrayBuffer[Long] currently.
 */
class BufferType extends UserDefinedType[Buffer] {
    def sqlType: DataType = ArrayType(LongType, true)

    def serialize(obj: Any): Any = obj match {
        case c: Buffer => c.toSeq
        case other => throw new UnsupportedOperationException(
            s"Cannot serialize object ${other}")
    }

    /** Convert a SQL datum to the user type */
    def deserialize(datum: Any): Buffer = datum match {
        case a: Seq[_] => a.toBuffer.asInstanceOf[Buffer]
        case other => throw new UnsupportedOperationException(
            s"Cannot deserialize object ${other}")
    }

    def userClass: Class[Buffer] = {
        classOf[Buffer]
    }

    override def defaultSize: Int = 1024
}

case object BufferType extends BufferType
