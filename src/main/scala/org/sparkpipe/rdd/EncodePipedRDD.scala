package org.sparkpipe.rdd

import java.nio.charset.CodingErrorAction
import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, Codec}
import scala.reflect.ClassTag
import scala.sys.process.{Process, ProcessIO}
import scala.util.control.NonFatal
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

/**
 * Piped RDD with encoding.
 * Encoding can be specified as string or Codec.
 * `strict` parameter allows to silence errors during reading. They will be returned as a part of
 * dataset with prefix "[ERROR]". Otherwise exception is thrown, for invalid command, as well as
 * malicious input.
 */
private[rdd] class EncodePipedRDD[T: ClassTag](
    prev: RDD[T],
    private val encoding: String,
    private val strict: Boolean
) extends RDD[String](prev) {

    /**
     * Allows to specify codec instead of encoding string.
     * although `on...` actions are dropped, specify them using `strict` parameter
     */
    def this(prev: RDD[T], codec: Codec, strict: Boolean) = this(prev, codec.toString, strict)

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        // rule to apply on incorrect command
        // if true, it will enforce exception, otherwise, it will silence it returning as output
        val cmdRule = strict
        // rule to apply on malformed exception
        val malformedRule = if (strict) CodingErrorAction.REPORT else CodingErrorAction.REPLACE

        // create buffers for output and errors, those are always string buffers
        val logbuf: ArrayBuffer[String] = ArrayBuffer()
        val errbuf: ArrayBuffer[String] = ArrayBuffer()
        // codec (applies only for output) and error codec
        val codec: Codec = Codec(encoding).onMalformedInput(malformedRule)
        val errCodec: Codec = Codec.UTF8.onMalformedInput(CodingErrorAction.REPLACE)

        for (elem <- firstParent[T].iterator(split, context)) {
            val logger: ProcessIO = new ProcessIO(
                (input => input.close()),
                (output => {
                    // output can fail with malformed exception or any arbitrary exception
                    // need to catch it and push to the error buffer
                    // use `toString` to fetch type of exception
                    try {
                        Source.fromInputStream(output)(codec).getLines().foreach(
                            line => logbuf.append(line)
                        )
                    } catch {
                        case NonFatal(e) => errbuf.append("[ERROR] " + e.toString())
                    }
                }),
                (err => {
                    // we apply UTF-8 encoding and REPLACE malformed rule for error, as it is a
                    // general case, and we have to return error without generating another
                    // exception.
                    Source.fromInputStream(err)(errCodec).getLines().foreach(
                        errline => errbuf.append("[ERROR] " + errline)
                    )
                }),
                daemonizeThreads=false
            )
            // execute process, similar to Java's `process.waitFor()`
            Process(elem.toString).run(logger).exitValue()

            // check for errors and strict rule, if something has happened, return first error
            if (cmdRule && errbuf.nonEmpty) {
                throw new Exception("Subprocess exited with 1. " + "Failed for elem: " +
                    elem.toString + ", reason: " + errbuf.head)
            }
        }
        // merge two buffers into one. Strategy: if error buffer is empty, return log buffer, else
        // merge them, even if log buffer is empty.
        val merged = if (errbuf.isEmpty) logbuf else logbuf ++ errbuf
        merged.toIterator
    }
}
