lazy val scala212 = "2.12.19"

lazy val supportedScalaVersions = List(scala212)

organization := "be.icteam"
name := "adobe-analytics-datafeed-datasource"

ThisBuild / homepage := Some(url("https://github.com/timvw/adobe-analytics-datafeed-datasource"))
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(Developer("timvw", "Tim Van Wassenhove", "tim@timvw.be", url("https://timvw.be")))

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / scalaVersion := scala212

ThisBuild / crossScalaVersions := supportedScalaVersions

val sparkVersion = "3.5.1"
ThisBuild / libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val scalaTestVersion = "3.2.18"
ThisBuild / libraryDependencies ++= List(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
