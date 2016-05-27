package org.sparkpipe.rdd

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.CodingErrorAction
import java.util.StringTokenizer
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, Codec}
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

private[rdd] class EncodePipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    encoding: String,
    strict: Boolean,
    envVars: Map[String, String],
    bufferSize: Int)
  extends RDD[String](prev) {

  require(command.nonEmpty,
    s"Tokenizer returned empty command from ${command.mkString("(", ", ", ")")}")
  require(encoding.nonEmpty, "Encoding is empty")

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(prev: RDD[T], command: String, encoding: String, strict: Boolean) =
    this(prev, EncodePipedRDD.tokenize(command), encoding, strict, Map(), 8192)

  def this(prev: RDD[T], command: String, codec: Codec, strict: Boolean) =
    this(prev, command, codec.toString, strict)

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    // set warning if `file.encoding` and `sun.jnu.encoding` properties do not match specified
    // encoding, also display how to resolve problem.
    val fileEncodingProp = Option(System.getProperty("file.encoding"))
    val sunJnuEncodingProp = Option(System.getProperty("sun.jnu.encoding"))
    if (!fileEncodingProp.isDefined || !sunJnuEncodingProp.isDefined ||
        !fileEncodingProp.get.equals(encoding) || !sunJnuEncodingProp.get.equals(encoding)) {
      logWarning("System encoding is different from " + encoding + ". Even encoding will " +
        "be applied, driver program may display different result. It is recommended to " +
        "set configuration options 'spark.driver.extraJavaOptions' and/or " +
        "'spark.executor.extraJavaOptions' to be '-Dfile.encoding=YOUR_VALUE " +
        "-Dsun.jnu.encoding=YOUR_VALUE'"
      )
    }

    // rule to apply on malformed exception
    val malformedRule = if (strict) CodingErrorAction.REPORT else CodingErrorAction.REPLACE
    val codec: Codec = Codec(encoding).onMalformedInput(malformedRule)
    val errCodec: Codec = Codec.UTF8.onMalformedInput(CodingErrorAction.REPLACE)
    var currentError = new AtomicReference[String](null)
    var currentItem = new AtomicReference[String](null)

    val pb = new ProcessBuilder(command.asJava)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    val proc = pb.start()
    val childThreadException = new AtomicReference[Throwable](null)

    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $command") {
      override def run(): Unit = {
        val err = proc.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(errCodec).getLines) {
            currentError.set(line)
            System.err.println(s"Error: $line")
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread(s"stdin writer for $command") {
      override def run(): Unit = {
        val out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(proc.getOutputStream), bufferSize))
        try {
          for (elem <- firstParent[T].iterator(split, context)) {
            val item = elem.toString
            require(item.nonEmpty, "Encountered empty element for input stream")
            currentItem.set(item)
            out.println(item)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val lines = Source.fromInputStream(proc.getInputStream)(codec).getLines()

    new Iterator[String] {
      def next(): String = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        lines.next()
      }

      def hasNext(): Boolean = {
        val result = if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            // look up error stream
            val err = currentError.get()
            val item = currentItem.get()
            val errMsg = s"Subprocess exited with status $exitStatus. " +
              s"Command ran: ${command.mkString("[", " ", "]")} for item $item, reason: $err"
            logError(errMsg)
            if (strict) {
              throw new IllegalStateException(errMsg)
            } else {
              // reset current error
              currentError.set(null)
              currentItem.set(null)
            }
          }
          false
        }
        propagateChildException()
        result
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          val commandRan = command.mkString(" ")
          logError(s"Caught exception while running pipe() operator. Command ran: $commandRan. " +
            s"Exception: ${t.getMessage}")
          proc.destroy()
          throw t
        }
      }
    }
  }
}

object EncodePipedRDD {
  // Split a string into words using a standard StringTokenizer
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
}
