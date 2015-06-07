import java.io.PrintWriter
import java.io.File
import scala.io.Source

/**
 * Pipe implementation in Scala.
 */

object PipeExample {
    def main(args: Array[String]) {
        // data to process
        val data = List("echo a", "ls -la", "cat ./resource/echo.sh")
        // complete path of echo.sh
        // val command = Seq("resource", "echo.sh").mkString(File.separator)
        val command = "bash"
        // proecess builder
        val pb = new ProcessBuilder(command)
        val proc = pb.start()
        // redirect proc stderr to System.err
        new Thread("stderr reader for " + command) {
            override def run() {
                for(line <- Source.fromInputStream(proc.getErrorStream).getLines) {
                    System.err.println(line)
                }
            }
        }.start()

        //pipe data to proc stdin
        new Thread("stdin writer for " + command) {
            override def run() {
                val out = new PrintWriter(proc.getOutputStream)
                for (elem <- data) {
                    out.println(elem)
                }
                out.close()
            }
        }.start()

        //read data from proc stdout
        val outputLines = Source.fromInputStream(proc.getInputStream).getLines
        println(outputLines.toList)
    }
}
