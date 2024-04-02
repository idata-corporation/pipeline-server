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
import net.idata.pipeline.common.model.{Notification, PipelineEnvironment}
import net.idata.pipeline.common.util.{GuidV5, NotificationUtil, ObjectStoreUtil}
import net.idata.pipeline.model._
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties
import scala.collection.JavaConverters._

class PostgresLoader(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[PostgresLoader])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the data into Postgres database: " + config.destination.database.dbName + ", table: " + config.destination.database.table)

        val secrets = SecretsUtil.postgresSecrets()

        Class.forName("org.postgresql.Driver")
        statusUtil.info("processing", "Postgres driver loaded successfully")

        var conn: Connection = null
        var statement: Statement = null

        try {
            val properties = new Properties()
            properties.setProperty("user", secrets.username)
            properties.setProperty("password", secrets.password)
            statusUtil.info("processing", "jdbc url: " + secrets.jdbcUrl)
            conn = DriverManager.getConnection(secrets.jdbcUrl, properties)
            statusUtil.info("processing", "Postgres connection acquired")
            statement = conn.createStatement()

            val file = createStagingFile()

            if(config.destination.database.truncateBeforeWrite) {
                statusUtil.info("processing", "'truncateTableBeforeWrite' is set to true, truncating table")
                statement.execute("truncate table " + config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table)
            }

            copyInto(conn, statement, file)

            sendNotification()
            statusUtil.info("end", "Process completed")
        } finally {
            if (statement != null)
                statement.close()
            if (conn != null)
                conn.close()
        }
    }

    private def createStagingFile(): String = {
        // Write the data to a temp location
        val tempUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/data/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".csv"
        val data = jobContext.data.rows.mkString("\n")
        ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(tempUrl), ObjectStoreUtil.getKey(tempUrl), data)
        tempUrl
    }

    private def copyInto(conn: Connection, statement: Statement, fileUrl: String): Unit = {
        statusUtil.info("processing", "Copying data into " + config.destination.database.table)

        if(!config.destination.database.manageTableManually)
            createTableIfUndefined(statement, config.destination.database.table)

        val sql = new StringBuilder()
        sql.append("COPY " + config.destination.database.dbName + "." + config.destination.database.schema + "." + config.destination.database.table + " FROM STDIN (")

        // Append the options (i.e. DELIMITER ',', FORMAT csv, etc)
        if(config.destination.database.options != null) {
            val options = config.destination.database.options.asScala.mkString(", ")
            sql.append(options)
        }
        else {
            // Default to CSV if no options are declared
            sql.append("FORMAT csv")
        }

        sql.append(")")

        statusUtil.info("processing", "Copy command: " + sql.toString())
        val (inputStream, s3Object) = ObjectStoreUtil.getInputStream(ObjectStoreUtil.getBucket(fileUrl), ObjectStoreUtil.getKey(fileUrl))
        val rowsInserted = new CopyManager(conn.asInstanceOf[BaseConnection])
            .copyIn(sql.mkString, inputStream)
        statusUtil.info("processing", "Rows inserted into table: " + rowsInserted.toString)

        inputStream.close()
        s3Object.close()
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
            if(field.name.compareToIgnoreCase("_json") == 0)
                sql.append("json, ")
            else if(field.name.compareToIgnoreCase("_xml") == 0)
                sql.append("xml, ")
            else if(field.`type`.compareToIgnoreCase("tinyint") == 0)
                sql.append("int2, ")
            else if(field.`type`.compareToIgnoreCase("smallint") == 0)
                sql.append("int2, ")
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

        statusUtil.info("processing", "Postgres create table statement: " + sql.mkString)
        statement.execute(sql.mkString)
    }

    private def sendNotification(): Unit = {
        val notification = Notification(
            config.name,
            jobContext.metadata.publisherToken,
            jobContext.pipelineToken,
            "postgres",
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
        attributes.put("destination", "postgres")
        attributes.put("schema", config.destination.database.schema)
        attributes.put("database", config.destination.database.dbName)
        attributes.put("table", config.destination.database.table)

        NotificationUtil.add(PipelineEnvironment.values.datasetTopicArn, jsonNotification, attributes.asScala.toMap)
        statusUtil.info("processing", "notification sent: " + jsonNotification)
    }
}
