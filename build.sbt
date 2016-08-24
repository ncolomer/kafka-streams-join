name := "kafka-streams-join"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.10.0.1",
  "org.apache.kafka" % "kafka-streams" % "0.10.0.1",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)