package org.sparkpipe.sql

import scala.collection.mutable.ArrayBuffer

/** package object to hold types alongside with custom DataType classes */
package object types {
    // define alias for BufferType
    type Buffer = ArrayBuffer[Long]
}
