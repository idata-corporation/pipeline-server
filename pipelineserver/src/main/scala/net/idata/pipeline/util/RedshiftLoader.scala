package net.idata.pipeline.util

/*
IData Pipeline
Copyright (C) 2024 IData Corporation (http://www.idata.net)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

import com.google.gson.Gson
import net.idata.pipeline.common.model.{Notification, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, NotificationUtil}
import net.idata.pipeline.model._
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Statement}
import java.time.Instant
import java.util.Properties
import scala.collection.JavaConverters._

class RedshiftLoader(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[RedshiftLoader])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil
    
    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the data into Redshift database: " + config.destination.database.dbName + ", table: " + config.destination.database.table)

        val secrets = SecretsUtil.redshiftSecrets()

        Class.forName("com.amazon.redshift.jdbc42.Driver")
        statusUtil.info("processing", "Redshift driver loaded successfully")

        var conn: Connection = null
        var statement: Statement = null

        try {
            val properties = new Properties()
            properties.setProperty("user", secrets.username)
            properties.setProperty("password", secrets.password)
            statusUtil.info("processing", "jdbc url: " + secrets.jdbcUrl)
            conn = DriverManager.getConnection(secrets.jdbcUrl, properties)
            statusUtil.info("processing", "Redshift connection acquired")
            statement = conn.createStatement()

            val stageUrl = prepareStagingFile()

            if(config.destination.database.truncateBeforeWrite) {
                statusUtil.info("processing", "'truncateTableBeforeWrite' is set to true, truncating table")
                statement.execute("truncate table " + config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table)
            }

            if(config.destination.database.keyFields != null)
                mergeInto(statement, secrets.dbRole, stageUrl)
            else
                copyInto(statement, secrets.dbRole, stageUrl)

            sendNotification()
            statusUtil.info("end", "Process completed")
        } finally {
            if (statement != null)
                statement.close()
            if (conn != null)
                conn.close()
        }
    }

    def executeSQL(sql: String): Unit = {
        val secrets = SecretsUtil.redshiftSecrets()

        Class.forName("com.amazon.redshift.jdbc42.Driver")

        var conn: Connection = null
        var statement: java.sql.Statement = null

        try {
            val properties = new Properties()
            properties.setProperty("user", secrets.username)
            properties.setProperty("password", secrets.password)
            logger.info("jdbc url: " + secrets.jdbcUrl)
            conn = DriverManager.getConnection(secrets.jdbcUrl, properties)
            logger.info("Redshift connection acquired")

            statement = conn.createStatement()
            statement.execute(sql)
        } finally {
            if (statement != null)
                statement.close()
            if (conn != null)
                conn.close()
        }
    }

    private def prepareStagingFile(): String = {
        // For JSON files ingested, use the original JSON file to load into Redshift
        if(config.source.fileAttributes.jsonAttributes != null) {
            val files = new DatasetMetadataUtil(jobContext.statusUtil).getFiles(jobContext.metadata)
            if(files.size > 1)
                throw new PipelineException("Redshift bulk file loading for JSON files is currently unsupported")
            files.head
        }
        else {
            // Convert source files to Parquet
            val stageUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/redshift/" + config.name + "." + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString)
            ParquetUtil.convertCSVs(jobContext, stageUrl, config)
            stageUrl
        }
    }

    private def copyInto(statement: Statement, dbRole: String, stageUrl: String): Unit = {
        statusUtil.info("processing", "Copying data into " + config.destination.database.table)

        if(!config.destination.database.manageTableManually)
            createTableIfUndefined(statement, config.destination.database.table)

        val command = new StringBuilder()
        if(config.source.fileAttributes.jsonAttributes != null) {
            command.append("COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table +
                " FROM '" + stageUrl + "'" +
                " CREDENTIALS '" + "aws_iam_role=" + dbRole + "'" +
                " FORMAT JSON 'noshred'"
            )
        }
        else {
            command.append("COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table +
                " FROM '" + stageUrl + "'" +
                " CREDENTIALS '" + "aws_iam_role=" + dbRole + "'" +
                " FORMAT AS PARQUET"
            )
        }
        statusUtil.info("processing", "Copy command: " + command.toString())

        statement.execute(command.toString())
    }

    private def mergeInto(statement: Statement, dbRole: String, stageUrl: String): Unit = {
        statusUtil.info("processing", "Merging data into " + config.destination.database.table)

        // Create the table
        if(!config.destination.database.manageTableManually)
            createTableIfUndefined(statement, config.destination.database.table)

        statement.execute("begin transaction")

        // Create the temp table
        val tempTableName = config.destination.database.table + "_" + System.currentTimeMillis().toString
        createTableIfUndefined(statement, tempTableName)

        // Copy the data into a temp table
        val tempSql = "COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + tempTableName +
            " FROM '" + stageUrl + "'" +
            " CREDENTIALS '" + "aws_iam_role=" + dbRole + "'" +
            " FORMAT AS PARQUET"
        statusUtil.info("processing", "Copy into temp table command: " + tempSql)
        statement.execute(tempSql)

        // Merge the tables by key fields
        val sql = new StringBuilder()
        val tableName = config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table
        sql.append("delete from " + tableName + " using " + tempTableName + " where ")
        config.destination.database.keyFields.forEach(keyField => {
            sql.append(tableName + "." + keyField + " = " + tempTableName + "." + keyField + " and ")
        })
        sql.setLength(sql.length - 4)
        statusUtil.info("processing", "SQL command to delete existing by key(s): " + sql.mkString)
        statement.execute(sql.mkString)
        statement.execute("insert into " + tableName + " select * from " + tempTableName)
        statement.execute("drop table " + tempTableName)

        statement.execute("end transaction")
    }

    private def createTableIfUndefined(statement: Statement, tableName: String): Unit = {
        val sql = new StringBuilder()

        // Begin
        val dbName = config.destination.database.dbName
        val schema = config.destination.database.schema
        sql.append("create table if not exists " + dbName + "." + schema + "." + tableName + " (")

        // Fields
        config.destination.schemaProperties.fields.forEach(field => {
            sql.append("\"" + field.name + "\" ")
            // Force the semi-structured field type to SUPER
            if(field.name.compareToIgnoreCase("_json") == 0 || field.name.compareToIgnoreCase("_xml") == 0)
                sql.append("super, ")
            else if(field.`type`.compareToIgnoreCase("tinyint") == 0)
                sql.append("int2, ")
            else if(field.`type`.compareToIgnoreCase("smallint") == 0)
                sql.append("int, ")
            else if(field.`type`.compareToIgnoreCase("float") == 0)
                sql.append("float4, ")
            else if(field.`type`.compareToIgnoreCase("double") == 0)
                sql.append("float8, ")
            else if(field.`type`.compareToIgnoreCase("string") == 0)
                sql.append("text, ")
            else
                sql.append(field.`type` + ", ")
        })
        sql.setLength(sql.length - 2)

        // Keys?
        if(config.destination.database.keyFields != null) {
            sql.append(", primary key (")
            config.destination.database.keyFields.forEach(field => {
                sql.append(field + ", ")
            })
            sql.setLength(sql.length - 2)
            sql.append(")")
        }

        // End
        sql.append(");")

        statusUtil.info("processing", "Redshift create table statement: " + sql.mkString)
        statement.execute(sql.mkString)
    }

    private def sendNotification(): Unit = {
        val notification = Notification(
            config.name,
            jobContext.metadata.publisherToken,
            jobContext.pipelineToken,
            "redshift",
            null,
            null,
            null,
            config.destination.database.schema,
            config.destination.database.dbName,
            config.destination.database.table
        )
        val gson = new Gson
        val jsonNotification = gson.toJson(notification)

        // Create the message attributes for the SNS filter policy
        val attributes = new java.util.HashMap[String, String]
        attributes.put("dataset", config.name)
        attributes.put("destination", "redshift")
        attributes.put("schema", config.destination.database.schema)
        attributes.put("database", config.destination.database.dbName)
        attributes.put("table", config.destination.database.table)

        NotificationUtil.add(PipelineEnvironment.values.datasetTopicArn, jsonNotification, attributes.asScala.toMap)
        statusUtil.info("processing", "notification sent: " + jsonNotification)
    }
}
