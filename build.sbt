import com.typesafe.sbt.packager.docker._

enablePlugins(JavaAppPackaging, DockerPlugin)

organization := "org.renci"

name := "ontology-kp"

version := "0.4"

licenses := Seq("MIT license" -> url("https://opensource.org/licenses/MIT"))

scalaVersion := "2.13.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

javaOptions += "-Xmx8G"

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

val zioVersion = "1.0.16"
val zioConfigVersion = "1.0.10"
val tapirVersion = "0.16.16"
val http4sVersion = "0.21.33"
val circeVersion = "0.14.2"
val logbackVersion = "1.2.11"

reStart / javaOptions += "-Xmx16G"

libraryDependencies ++= {
  Seq(
    "dev.zio"                     %% "zio"                      % zioVersion,
    "dev.zio"                     %% "zio-interop-cats"         % "2.5.1.0",
    "dev.zio"                     %% "zio-config"               % zioConfigVersion,
    "dev.zio"                     %% "zio-config-magnolia"      % zioConfigVersion,
    "dev.zio"                     %% "zio-config-typesafe"      % zioConfigVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-core"               % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-zio"                % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-zio-http4s-server"  % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe"         % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs"       % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % tapirVersion,
    "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-http4s"  % tapirVersion,
    "org.http4s"                  %% "http4s-blaze-server"      % http4sVersion,
    "org.http4s"                  %% "http4s-dsl"               % http4sVersion,
    "org.http4s"                  %% "http4s-blaze-client"      % http4sVersion,
    "org.http4s"                  %% "http4s-circe"             % http4sVersion,
    "org.apache.jena"              % "apache-jena-libs"         % "4.5.0",
    "org.phenoscape"              %% "sparql-utils"             % "1.3.1",
    "org.apache.commons"           % "commons-text"             % "1.9",
    "commons-codec"                % "commons-codec"            % "1.15",
    "io.circe"                    %% "circe-core"               % circeVersion,
    "io.circe"                    %% "circe-generic"            % circeVersion,
    "io.circe"                    %% "circe-parser"             % circeVersion,
    "io.circe"                    %% "circe-yaml"               % "0.14.1",
    "dev.zio"                     %% "zio-test"                 % zioVersion % Test,
    "dev.zio"                     %% "zio-test-sbt"             % zioVersion % Test,
    "ch.qos.logback"               % "logback-classic"          % logbackVersion,
    "com.typesafe.scala-logging"  %% "scala-logging"            % "3.9.5"
  )
}

dockerBaseImage := "openjdk:17-buster"
dockerUsername := Some("renciorg")
dockerExposedPorts := Seq(8080)
dockerUpdateLatest := true
