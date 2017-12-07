
name := "datalab-employee"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
    "com.typesafe" % "config" % "1.3.1",
    "com.github.scopt" %% "scopt" % "3.6.0",
    "org.vegas-viz" %% "vegas" % "0.3.9",
    "org.vegas-viz" %% "vegas-spark" % "0.3.9",
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
)

assemblyOutputPath in assembly := file(s"${baseDirectory.value.getAbsolutePath}/jar/${name.value}.jar")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf"            => MergeStrategy.concat
    case _ => MergeStrategy.first
}
