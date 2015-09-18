name := "sparkpipe-experimental"

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

parallelExecution in Test := false

// assembly settings
assemblyJarName in assembly := "sparkpipe-experimental.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "pom.xml" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.first
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}
