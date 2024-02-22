/*
 Copyright 2024 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

name := "pipeline-server"
ThisBuild / organization := "net.idata"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "2.3.4"

lazy val root = (project in file(".")).
    enablePlugins(BuildInfoPlugin).
    settings(
        buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
        buildInfoPackage := "net.idata.pipeline.build.sbt"
    )

val awsCoreV = "1.12.286"

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.15" % Test,
    "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
    "com.amazonaws" % "aws-java-sdk-core" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-s3" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-sqs" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-sns" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-dynamodb" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-glue" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-athena" % awsCoreV,
    "com.amazonaws" % "aws-java-sdk-marketplacemeteringservice" % awsCoreV,
    "com.google.guava" % "guava" % "30.1-jre",
    "com.google.code.gson" % "gson" % "2.8.6",
    "com.jayway.jsonpath" % "json-path" % "2.5.0",
    "org.apache.commons" % "commons-lang3" % "3.11",
    "commons-io" % "commons-io" % "2.8.0",
    "org.apache.commons" % "commons-compress" % "1.21",
    "commons-fileupload" % "commons-fileupload" % "1.4",
    "org.everit.json" % "org.everit.json.schema" % "1.5.1",
    "org.springframework.boot" % "spring-boot-starter" % "2.6.7",
    "org.springframework.boot" % "spring-boot-starter-web" % "2.6.7",
    "net.snowflake" % "snowflake-jdbc" % "3.13.23",
    "org.apache.commons" % "commons-csv" % "1.9.0",
    "io.deephaven" % "deephaven-csv" % "0.8.0",
    "org.quartz-scheduler" % "quartz" % "2.3.2",
    "org.postgresql" % "postgresql" % "42.6.0",
    "com.microsoft.sqlserver" % "mssql-jdbc" % "12.2.0.jre8",
    "mysql" % "mysql-connector-java" % "8.0.32",
    "org.apache.kafka" %% "kafka" % "3.5.1",
    "org.apache.kafka" % "kafka-clients" % "3.5.1"
)

Compile / unmanagedJars += file("./jars/redshift-jdbc42-2.1.0.9.jar")

assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "spring.factories") => MergeStrategy.filterDistinctLines
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
}