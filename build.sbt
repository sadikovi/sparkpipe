name := "sparkpipe-experimental"

version := "0.1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided",
    "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
    "org.jblas" % "jblas" % "1.2.4"
)

scalacOptions in (Compile, doc) ++= Seq(
    "-groups", "-implicits", "-deprecation", "-unchecked"
)

// assembly settings
assemblyJarName in assembly := "sparkpipe-experimental.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

parallelExecution in Test := false
