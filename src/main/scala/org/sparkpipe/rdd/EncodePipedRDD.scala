package org.sparkpipe.rdd

import java.io.IOException
import java.nio.charset.CodingErrorAction
import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, Codec}
import scala.reflect.ClassTag
import scala.sys.process.{Process, ProcessBuilder, ProcessIO}
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

        // rule to apply on incorrect command
        // if true, it will enforce exception, otherwise, it will silence it returning as output
        val executionRule = strict
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
            // tokenize complex command
            val cmds = EncodePipedRDD.tokenize(elem.toString)
            // create buffered process from all the commands
            var bufferedProcess: ProcessBuilder = Process(cmds.head)
            cmds.drop(1).foreach(
                cmd => {
                    println(cmd)
                    bufferedProcess = bufferedProcess.#|(Process(cmd))
                }
            )
            // execute process, similar to Java's `process.waitFor()`
            val exit = bufferedProcess.run(logger).exitValue()

            // check for errors and strict rule, if something has happened, return first error
            if (executionRule && errbuf.nonEmpty) {
                throw new IOException("Subprocess exited with status " + exit.toString + ". " +
                    "Failed for elem: " + elem.toString + ", reason: " + errbuf.head)
            }
        }
        // merge two buffers into one. Strategy: if error buffer is empty, return log buffer, else
        // merge them, even if log buffer is empty.
        val merged = if (errbuf.isEmpty) logbuf else logbuf ++ errbuf
        merged.toIterator
    }
}

private[rdd] object EncodePipedRDD {
    /**
     * Splits command by pipe into several simple commands for processes
     * e.g. cat temp/sample.log | grep -i "\" | a" will become
     * Array(cat temp/sample.log, grep -i "\" | a")
     * Each simple command is an array of tokens, cmd surrounding quotes are removed
     */
    def tokenize(cmd: String): ArrayBuffer[Array[String]] = {
        var isDoubleQuoted = false
        var isSingleQuoted = false
        val cmdbuf = ArrayBuffer[Array[String]]()
        val tmpbuf = new ArrayBuffer[String]()
        val token = new StringBuilder()
        var prevchr = '#'

        // whether current state is quoted, so we need to ignore pipe characters
        def isQuoted(): Boolean = isDoubleQuoted || isSingleQuoted

        // add newly created command from temp buffer
        // remove all empty strings from sequence
        // clear temp buffer to prepare for the next
        def flushCmd() {
            cmdbuf.append(tmpbuf.filter(_.nonEmpty).toArray)
            tmpbuf.clear()
        }

        // add read token to the a current command buffer, and clear the token
        def flushToken() {
            tmpbuf.append(token.toString())
            token.clear()
        }

        for (chr <- cmd) {
            if (!isQuoted() && chr == '|') {
                flushToken()
                flushCmd()
            } else if (!isQuoted() && (chr == 9 || chr == 10 || chr == 11 || chr == 12 ||
                chr == 13 || chr == 32 || chr == 160)) {
                // if `chr` is a whitespace character we flush token
                // instead of using scala.runtime.RichChar, we manually compare `chr` to list of
                // Latin spaces: https://en.wikipedia.org/wiki/Whitespace_character
                flushToken()
            } else {
                if (!isQuoted()) {
                    if (chr == '"') {
                        isDoubleQuoted = true
                    } else if (chr == '\'') {
                        isSingleQuoted = true
                    } else {
                        token.append(chr)
                    }
                } else {
                    if (isSingleQuoted && chr == '\'' && prevchr != '\\') {
                        isSingleQuoted = false
                    } else if (isDoubleQuoted && chr == '"' && prevchr != '\\') {
                        isDoubleQuoted = false
                    } else {
                        token.append(chr)
                    }
                }
                prevchr = chr
            }
        }
        // push the last command into cmd buffer
        flushToken()
        flushCmd()
        // and return complete command buffer
        cmdbuf
    }
}
