ThisBuild / version := "0.1.0-SNAPSHOT"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"
ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "RuleEngine"
  )
