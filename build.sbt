name := "UserLogDataAnalysis"

version := "0.1"

scalaVersion := "2.12.10"

//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test
libraryDependencies += "org.mockito" % "mockito-all" % "1.9.0-rc1" % Test
libraryDependencies += "org.powermock" % "powermock-module-junit4" % "1.4.12" % Test
libraryDependencies += "org.powermock" % "powermock-api-mockito" % "1.4.12" % Test

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
