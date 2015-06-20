name := "sparkpipe-app"

version := "0.0.1"

scalaVersion := "2.10.4"

// library dependencies for spark
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
)
