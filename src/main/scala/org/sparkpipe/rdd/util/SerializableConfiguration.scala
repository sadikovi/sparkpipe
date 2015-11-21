package org.sparkpipe.rdd.util

import java.io.{ObjectInputStream, ObjectOutputStream}
import org.apache.hadoop.conf.Configuration

/**
 * Serializable hadoop configuration. Clone of `org.apache.spark.util.SerializableConfiguration`,
 * since it cannot be reused outside `spark` package.
 */
private[rdd] class SerializableConfiguration(
    @transient var value: Configuration
) extends Serializable {
    private def writeObject(out: ObjectOutputStream): Unit = {
        out.defaultWriteObject()
        value.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = {
        value = new Configuration(false)
        value.readFields(in)
    }
}
