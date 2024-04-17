name := "pipeline-server"
ThisBuild / organization := "net.idata"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / version := "2.3.6"

lazy val global = project
    .in(file("."))
    .disablePlugins(AssemblyPlugin)
    .enablePlugins(BuildInfoPlugin)
    .settings(
        buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
        buildInfoPackage := "net.idata.pipeline.build.sbt"
    )
    .aggregate(
        common,
        pipelineserver,
        transform,
    )

lazy val common = project
    .settings(
        name := "common",
        assemblySettings,
        libraryDependencies ++= commonDependencies
    )

lazy val pipelineserver = project
    .settings(
        name := "pipelineserver",
        assemblySettings,
        libraryDependencies ++= commonDependencies,
        libraryDependencies ++= commonSpringDependencies,
        Compile / unmanagedJars += file("./jars/redshift-jdbc42-2.1.0.9.jar")
    )
    .dependsOn(
        common
    )

lazy val transform = project
    .settings(
        name := "transform",
        assemblySettingsSpark,
        libraryDependencies ++= commonSparkDependencies,
        libraryDependencies ++= commonTransformSparkDependencies,
        Compile / unmanagedJars += file("./jars/redshift-jdbc42-2.0.0.4.jar")
    )
    .dependsOn(
        common
    )

lazy val dependencies =
    new {
        val awsCoreV = "1.12.286"
        //val awsCoreV = "1.11.655"   // This version must be used for a Databricks Spark cluster

        val beanutils           = "commons-beanutils" % "commons-beanutils" % "1.9.4"
        val awscore             = "com.amazonaws" % "aws-java-sdk-core" % awsCoreV
        val awss3               = "com.amazonaws" % "aws-java-sdk-s3" % awsCoreV
        val awssqs              = "com.amazonaws" % "aws-java-sdk-sqs" % awsCoreV
        val awssns              = "com.amazonaws" % "aws-java-sdk-sns" % awsCoreV
        val awsdynamodb         = "com.amazonaws" % "aws-java-sdk-dynamodb" % awsCoreV
        val awssecretsmanager   = "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsCoreV
        val awsathena           = "com.amazonaws" % "aws-java-sdk-athena" % awsCoreV
        val awsglue             = "com.amazonaws" % "aws-java-sdk-glue" % awsCoreV
        val awssts              = "com.amazonaws" % "aws-java-sdk-sts" % awsCoreV
        val awsmarketplace      = "com.amazonaws" % "aws-java-sdk-marketplacemeteringservice" % awsCoreV
        val awsemr              = "com.amazonaws" % "aws-java-sdk-emr" % awsCoreV
        val awsemrcontainers    = "com.amazonaws" % "aws-java-sdk-emrcontainers" % "1.12.60"
        val googleguava         = "com.google.guava" % "guava" % "33.0.0-jre"
        val googlegson          = "com.google.code.gson" % "gson" % "2.9.1"
        val jaywayjsonpath      = "com.jayway.jsonpath" % "json-path" % "2.5.0"
        val apachecommonslang3  = "org.apache.commons" % "commons-lang3" % "3.11"
        val apachecommonsio     = "commons-io" % "commons-io" % "2.8.0"
        val apachecommonscompress = "org.apache.commons" % "commons-compress" % "1.21"
        val commonsfileupload   = "commons-fileupload" % "commons-fileupload" % "1.4"
        val commonscsv          = "org.apache.commons" % "commons-csv" % "1.9.0"
        val everitjsonschema    = "org.everit.json" % "org.everit.json.schema" % "1.5.1"
        val quartz              = "org.quartz-scheduler" % "quartz" % "2.3.2"
        val snowflake           = "net.snowflake" % "snowflake-jdbc" % "3.14.1"
        val deephaven           = "io.deephaven" % "deephaven-csv" % "0.8.0"
        val postgres            = "org.postgresql" % "postgresql" % "42.6.0"
        val mssql               = "com.microsoft.sqlserver" % "mssql-jdbc" % "12.6.0.jre11"
        val mysql               = "mysql" % "mysql-connector-java" % "8.0.32"
        val kafka               = "org.apache.kafka" %% "kafka" % "3.5.1"
        val kafkaclients        = "org.apache.kafka" % "kafka-clients" % "3.5.1"
        val debezium            = "io.debezium" % "debezium-core" % "2.5.2.Final"
    }

lazy val springDependencies =
    new {
        val springbootstarter       = "org.springframework.boot" % "spring-boot-starter" % "2.6.7"
        val springbootstarterweb    = "org.springframework.boot" % "spring-boot-starter-web" % "2.6.7"
    }

lazy val sparkDependencies =
    new {
        val sparkcore   = "org.apache.spark" %% "spark-core" % "3.1.1"
        val sparksql    = "org.apache.spark" %% "spark-sql" % "3.1.1"
        val sparkhive   = "org.apache.spark" %% "spark-hive" % "3.1.1"
        val deltacore   = "io.delta" %% "delta-core" % "1.0.0"
        val apachepoi   = "org.apache.poi" % "poi" % "5.0.0"
        val apachepoixml = "org.apache.poi" % "poi-ooxml" % "5.0.0"
    }

lazy val commonDependencies = Seq(
    dependencies.beanutils,
    dependencies.awscore,
    dependencies.awss3,
    dependencies.awssqs,
    dependencies.awssns,
    dependencies.awsdynamodb,
    dependencies.awssecretsmanager,
    dependencies.awsathena,
    dependencies.awsglue,
    dependencies.awssts,
    dependencies.awsmarketplace,
    dependencies.awsemr,
    dependencies.awsemrcontainers,
    dependencies.googleguava,
    dependencies.googlegson,
    dependencies.jaywayjsonpath,
    dependencies.apachecommonslang3,
    dependencies.apachecommonsio,
    dependencies.apachecommonscompress,
    dependencies.commonsfileupload,
    dependencies.commonscsv,
    dependencies.everitjsonschema,
    dependencies.quartz,
    dependencies.snowflake,
    dependencies.deephaven,
    dependencies.postgres,
    dependencies.mssql,
    dependencies.mysql,
    dependencies.kafkaclients,
    dependencies.debezium
)

lazy val commonSpringDependencies = Seq(
    springDependencies.springbootstarter,
    springDependencies.springbootstarterweb
)

lazy val commonSparkDependencies = Seq(
    sparkDependencies.sparkcore % "provided",
    sparkDependencies.sparksql % "provided",
    sparkDependencies.sparkhive % "provided"
)

lazy val commonTransformSparkDependencies = Seq(
    sparkDependencies.apachepoi,
    sparkDependencies.apachepoixml
)

lazy val assemblySettings = Seq(
    assembly / assemblyJarName := ("pipeline-" + name.value + "-assembly-" + version.value + ".jar"),
    assembly / assemblyMergeStrategy := {
        case PathList("META-INF", "spring.factories") => MergeStrategy.filterDistinctLines
        case PathList("META-INF", _*) => MergeStrategy.discard
        case _ => MergeStrategy.first
    }
)

lazy val assemblySettingsSpark = Seq(
    assembly / assemblyJarName := ("pipeline-" + name.value + "-assembly-" + version.value + ".jar"),
    assembly / assemblyMergeStrategy := {
        case PathList("META-INF", xs @ _*) =>
            (xs map {_.toLowerCase}) match {
                case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
                    MergeStrategy.discard
                case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa")  =>
                    MergeStrategy.discard
                case _ => MergeStrategy.first // Changed deduplicate to first
            }
        case _ => MergeStrategy.first
    }
)