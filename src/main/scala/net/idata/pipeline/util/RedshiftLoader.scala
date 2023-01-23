package net.idata.pipeline.util

/*
IData Pipeline
Copyright (C) 2023 IData Corporation (http://www.idata.net)

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
import net.idata.pipeline.model.{JobContext, Notification, PipelineEnvironment, PipelineException}
import net.idata.pipeline.util.aws.SecretsManagerUtil
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Statement}
import java.time.Instant
import java.util.Properties

class RedshiftLoader(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[RedshiftLoader])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil
    
    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the data into Redshift database: " + config.destination.database.dbName + ", table: " + config.destination.database.table)

        val dbSecret = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.redshiftSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager"))
        val username = dbSecret.get("username")
        if(username == null)
            throw new PipelineException("Could not retrieve the Redshift username from Secrets Manager")
        val password = dbSecret.get("password")
        if(password == null)
            throw new PipelineException("Could not retrieve the Redshift password from Secrets Manager")
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if(jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Redshift jdbcUrl from Secrets Manager")
        val dbRole = dbSecret.get("dbRole")
        if(dbRole == null)
            throw new PipelineException("Could not retrieve the Redshift dbRole from Secrets Manager")

        Class.forName("com.amazon.redshift.jdbc42.Driver")
        logger.info("Redshift driver loaded successfully")
        statusUtil.info("processing", "Redshift driver loaded successfully")

        var conn: Connection = null
        var statement: Statement = null

        try {
            val properties = new Properties()
            properties.setProperty("user", username)
            properties.setProperty("password", password)
            logger.info("jdbc url: " + jdbcUrl)
            conn = DriverManager.getConnection(jdbcUrl, properties)
            logger.info("Redshift connection acquired")
            statement = conn.createStatement()

            val stageUrl = prepareStagingFile()

            if(config.destination.database.redshift.keyFields != null)
                mergeInto(statement, dbRole, stageUrl)
            else
                copyInto(statement, dbRole, stageUrl)

            sendNotification()
            statusUtil.info("end", "Process completed")
        } finally {
            if (statement != null)
                statement.close()
            if (conn != null)
                conn.close()
        }
    }

    private def prepareStagingFile(): String = {
        val files = DatasetMetadataUtil.getFiles(jobContext.metadata)

        // For JSON files ingested, use the original JSON file to load into Redshift
        if(config.source.fileAttributes.jsonAttributes != null) {
            if(files.size > 1)
                throw new PipelineException("Redshift bulk file loading for JSON files is currently unsupported")
            files.head
        }
        else {
            // Convert source files to Parquet
            val stageUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/redshift/" + config.name + "." + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString)
            ParquetUtil.convertCSVs(stageUrl, config, jobContext.metadata)
            stageUrl
        }
    }

    private def copyInto(statement: Statement, dbRole: String, stageUrl: String, tempTableName: String = null, createTable: Boolean = true): Unit = {
        val table = {
            if(tempTableName == null)
                config.destination.database.table
            else
                tempTableName
        }

        if(createTable)
            createTableIfUndefined(statement, table)

        statusUtil.info("processing", "Copying data into " + table)

        val command = new StringBuilder()
        if(config.source.fileAttributes.jsonAttributes != null) {
            command.append("COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + table +
                " FROM '" + stageUrl + "'" +
                " CREDENTIALS '" + "aws_iam_role=" + dbRole + "'" +
                " FORMAT JSON 'noshred'"
            )
        }
        else {
            command.append("COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + table +
                " FROM '" + stageUrl + "'" +
                " CREDENTIALS '" + "aws_iam_role=" + dbRole + "'" +
                " FORMAT AS PARQUET'"
            )
        }
        logger.info("Copy command: " + command.toString())

        statement.execute(command.toString())
    }

    private def mergeInto(statement: Statement, dbRole: String, stageUrl: String): Unit = {
        val tempTableName = config.destination.database.table + "_" + System.currentTimeMillis().toString

        // Create the table
        createTableIfUndefined(statement, config.destination.database.table)

        statusUtil.info("processing", "Merging data into " + config.destination.database.table)

        // Create the temp table
        statement.execute("begin transaction")
        createTableIfUndefined(statement, tempTableName)

        // Copy the data into a temp table
        copyInto(statement, dbRole, stageUrl, tempTableName, createTable = false)

        // Merge the tables by key fields
        val sql = new StringBuilder()
        val tableName = config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table
        sql.append("delete from " + tableName + " using " + tempTableName + " where ")
        config.destination.database.redshift.keyFields.forEach(keyField => {
            sql.append(tableName + "." + keyField + " = " + tempTableName + "." + keyField + " and ")
        })
        sql.setLength(sql.length - 4)
        logger.info("SQL command to delete existing by key(s): " + sql.mkString)
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
        if(config.destination.database.redshift.keyFields != null) {
            sql.append(", primary key (")
            config.destination.database.redshift.keyFields.forEach(field => {
                sql.append(field + ", ")
            })
            sql.setLength(sql.length - 2)
            sql.append(")")
        }

        // End
        sql.append(");")

        logger.info("Redshift create table statement: " + sql.mkString)
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
        logger.info("notification sent: " + jsonNotification)
        NotificationUtil.add(PipelineEnvironment.values.notifyTopicArn, jsonNotification)
    }
}
