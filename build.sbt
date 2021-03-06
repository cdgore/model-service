resolvers += "spray repo" at "http://repo.spray.io"

val sprayVersion = "1.3.3"

lazy val buildSettings = Seq(
  name := "model-service",
  version := "1.0-SNAPSHOT",
  organization := "com.cdgore",
  scalaVersion := "2.11.6"
)

lazy val app = (project in file(".")).
  settings(buildSettings: _*).
  settings(
    mainClass in assembly := Some("modelservice.Boot")
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.14",
//  "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
  "io.spray" %% "spray-can" % sprayVersion,
  "io.spray" %% "spray-testkit" % sprayVersion % "test",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "joda-time" % "joda-time" % "2.7"
)