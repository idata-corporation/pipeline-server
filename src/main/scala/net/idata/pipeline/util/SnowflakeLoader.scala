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

Author(s): Todd Fearn
*/

import com.google.gson.Gson
import net.idata.pipeline.model._
import net.idata.pipeline.util.aws.SecretsManagerUtil
import net.snowflake.client.core.QueryStatus
import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeStatement}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.Instant
import java.util.Properties
import scala.collection.JavaConverters._

class SnowflakeLoader(jobContext: JobContext) {
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the data into Snowflake database: " + config.destination.database.dbName + ", table: " + config.destination.database.table)

        // Load the snowflake driver
        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

        val dbSecret = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.snowflakeSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.snowflakeSecretName))
        val username = dbSecret.get("username")
        if(username == null)
            throw new PipelineException("Could not retrieve the Snowflake username from Secrets Manager")
        val password = dbSecret.get("password")
        if(password == null)
            throw new PipelineException("Could not retrieve the Snowflake password from Secrets Manager")
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if(jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Snowflake jdbcUrl from Secrets Manager")
        val stageName = dbSecret.get("stageName")
        if(stageName == null)
            throw new PipelineException("Could not retrieve the Snowflake stageName from Secrets Manager")

        var conn: Connection = null
        var statement: Statement = null
        try {
            val properties = new Properties()
            properties.setProperty("user", username)
            properties.setProperty("password", password)
            properties.setProperty("warehouse", config.destination.database.snowflake.warehouse)
            properties.setProperty("db", config.destination.database.dbName)
            properties.setProperty("schema", config.destination.database.schema)
            conn = DriverManager.getConnection(jdbcUrl, properties)
            statusUtil.info("processing", "Snowflake connection acquired")
            statement = conn.createStatement()

            if(!config.destination.database.manageTableManually)
                createTableIfUndefined(statement)

            val snowflakeStageUrl = prepareStagingFile()

            processFromStage(stageName, snowflakeStageUrl, statement)

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
        val stageUrl = config.name + "." + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + "/"
        val tempFilesUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/snowflake/" + stageUrl
        if(useParquetStagingFile())
            prepareParquetStagingFile(tempFilesUrl)
        else
            prepareCsvStagingFile(tempFilesUrl)
        stageUrl
    }

    private def prepareParquetStagingFile(stageUrl: String): Unit = {
        statusUtil.info("processing", "Preparing parquet staging file(s) at location: " + stageUrl)

        ParquetUtil.convertCSVs(jobContext, stageUrl, config)
    }

    private def prepareCsvStagingFile(stageUrl: String): String = {
        statusUtil.info("processing", "Preparing CSV staging file at location: " + stageUrl)

        // If the incoming file is a CSV read the file and filter out the header and appropriate columns based upon the destination schema
        if(config.source.fileAttributes.csvAttributes != null) {
            // Write the data to a temp location
            val tempUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/data/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".csv"
            val data = jobContext.data.rows.mkString("\n")
            ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(tempUrl), ObjectStoreUtil.getKey(tempUrl), data)

            // Read it back based upon the destination schema order
            val file = new CSVReader().readFile(tempUrl,
                header = false,
                config.source.fileAttributes.csvAttributes.delimiter,
                config.source.schemaProperties.fields.asScala.map(_.name).toList,
                config.destination.schemaProperties.fields.asScala.map(_.name).toList)

            // Write the file to the stage
            ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(stageUrl), ObjectStoreUtil.getKey(stageUrl), file)
        }
        else {
            // Otherwise, just copy the raw file to the staging bucket
            val files = new DatasetMetadataUtil(statusUtil).getFiles(jobContext.metadata)
            val fileUrl = files.head
            ObjectStoreUtil.copyBucketObject(ObjectStoreUtil.getBucket(fileUrl),
                ObjectStoreUtil.getKey(fileUrl),
                ObjectStoreUtil.getBucket(stageUrl),
                ObjectStoreUtil.getKey(stageUrl))
        }

        stageUrl
    }

    private def useParquetStagingFile(): Boolean = {
        // If incoming CSV file and not a Snowflake MERGE INTO, true
        config.source.fileAttributes.csvAttributes != null && config.destination.database.snowflake.keyFields == null
    }

    private def processFromStage(stageName: String, stageUrl: String, statement: Statement): Unit = {
        // Use warehouse, DB, schema name
        statement.execute("USE WAREHOUSE " + config.destination.database.snowflake.warehouse)
        statement.execute("USE " + config.destination.database.dbName)
        statement.execute("USE SCHEMA " + config.destination.database.schema)

        if(config.destination.database.truncateBeforeWrite) {
            statusUtil.info("processing", "'truncateTableBeforeWrite' is set to true, truncating table")
            statement.execute("truncate table " + config.destination.database.table)
        }

        val fileFormat = buildFileFormat()
        statusUtil.info("processing","File format SQL command: " + fileFormat)
        statement.executeUpdate(fileFormat)

        // Override?
        val sql = {
            if (config.destination.database.snowflake.sqlOverride != null) {
                val stage = createStage(stageUrl, stageName)
                val fileFormat = createFileFormat()
                config.destination.database.snowflake.sqlOverride.replace("@stage", stage) + fileFormat
            }
            else {
                // JSON or XML?
                if(config.source.fileAttributes.jsonAttributes != null ||  config.source.fileAttributes.xmlAttributes != null)
                    buildCopy(stageUrl, stageName)
                else if(config.destination.database.snowflake.keyFields != null)
                    buildMerge(stageUrl, stageName)
                else
                    buildCopy(stageUrl, stageName)
            }
        }
        statusUtil.info("processing","SQL command: " + sql)
        statement.executeUpdate(sql)
    }

    private def buildFileFormat(): String = {
        if(config.source.fileAttributes.jsonAttributes != null)
            "create or replace file format pipelinefileformat type = 'JSON'"
        else if(config.source.fileAttributes.xmlAttributes != null)
            "create or replace file format pipelinefileformat type = 'XML'"
        else if(config.source.fileAttributes.csvAttributes != null) {
            val sql = new StringBuilder()

            if(config.destination.database.snowflake.keyFields == null) {
                // Using a parquet file as input, create the parquet options
                sql.append("create or replace file format pipelinefileformat type = 'parquet'")

                // If there aren't any format type options, add SNAPPY as the file compression
                if(config.destination.database.snowflake.formatTypeOptions == null)
                    sql.append(" compression = 'snappy'")
            }
            else {
                // Using a CSV file as input, create the CSV options
                val delimiter = {
                    if(config.source.fileAttributes.csvAttributes != null)
                        config.source.fileAttributes.csvAttributes.delimiter
                    else
                        "|"
                }
                sql.append("create or replace file format pipelinefileformat type = 'csv' field_delimiter = '" + delimiter + "'")
            }

            // Format options?
            if(config.destination.database.snowflake.formatTypeOptions != null) {
                config.destination.database.snowflake.formatTypeOptions.forEach(option => {
                    sql.append(" " + option)
                })
            }
            sql.toString
        }
        else {
            throw new PipelineException("Internal error, Snowflake buildFileFormat() method has invalid input")
        }
    }

    private def buildCopy(stageUrl: String, stageName: String): String = {
        val stage = createStage(stageUrl, stageName)
        val fileFormat = createFileFormat()

        val copy = new StringBuilder()

        // If we're building a COPY INTO for a CSV file, assume it has been converted to parquet for ingestion into Snowflake
        if(config.source.fileAttributes.csvAttributes != null) {
            copy.append("COPY INTO " + config.destination.database.table + " FROM (SELECT ")
            config.destination.schemaProperties.fields.asScala.foreach(field => {
                copy.append("$1:" + field.name + "::" + field.`type` + ", ")
            })
            copy.setLength(copy.length - 2)
            copy.append(" FROM " + stage + ")" + fileFormat)
        }
        else {
            copy.append("COPY INTO " + config.destination.database.table)
            copy.append(" FROM " + stage + fileFormat)
        }

        copy.toString
    }

    private def buildMerge(stageUrl: String, stageName: String): String = {
        val merge = new StringBuilder()

        val schemaProperties = config.destination.schemaProperties

        merge.append("MERGE INTO ")
        merge.append(config.destination.database.table)
        merge.append(" USING (SELECT ")

        val range = 1 to schemaProperties.fields.size()
        range.foreach(i => {
            val fieldName = "$" + i.toString + " " + schemaProperties.fields.get(i-1).name + ","
            merge.append(fieldName)
        })
        merge.deleteCharAt(merge.length-1)

        // Stage
        merge.append(" FROM ")
        val stage = createStage(stageUrl, stageName)
        merge.append(stage)

        // File format
        val fileFormat = createFileFormat()
        merge.append(fileFormat)

        // Temp table name
        val tempTable = "temp"
        merge.append(") ")
        merge.append(tempTable)
        merge.append(" ON ")

        // Key(s)
        config.destination.database.snowflake.keyFields.forEach(keyField => {
            merge.append(config.destination.database.table + "." + keyField + " = " + tempTable + "." + keyField + " AND ")
        })
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)

        // When matched
        merge.append("WHEN MATCHED THEN UPDATE SET ")
        schemaProperties.fields.forEach(field => {
            merge.append(field.name + " = " + tempTable + "." + field.name + ", ")
        })
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)

        // When not matched
        merge.append(" WHEN NOT MATCHED THEN INSERT (")
        schemaProperties.fields.forEach(field => {
            merge.append(field.name + ", ")
        })
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)
        merge.append(") VALUES (")
        schemaProperties.fields.forEach(field => {
            merge.append(tempTable + "." + field.name + ", ")
        })
        merge.deleteCharAt(merge.length-1)
        merge.deleteCharAt(merge.length-1)
        merge.append(")")

        merge.toString
    }

    private def createStage(stageUrl: String, stageName: String): String = {
        // Sample stageUrl: [dataset-name].22ba8738-ed5b-57de-a6a5-ee18f0f6264a/
        "'@" + stageName + "/" + stageUrl + "'"
    }

    private def createFileFormat(): String = {
        if(config.destination.database.snowflake.keyFields != null)
            " (FILE_FORMAT => 'pipelinefileformat')"
        else
            " FILE_FORMAT = (FORMAT_NAME = 'pipelinefileformat')"
    }

    private def createTableIfUndefined(statement: Statement): Unit = {
        val tableName = config.destination.database.table
        val resultSet = doesTableExist(tableName, statement)
        if(resultSet == null)
            createTable(tableName, statement)
    }

    private def doesTableExist(tableName: String, statement: Statement): ResultSet = {
        val sql = "describe table " + tableName
        try {
            val resultSet = asyncQuery(statement, sql)
            if(resultSet.next)
                resultSet
            else
                null
        }
        catch {
            case e: Exception => null
        }
    }

    private def createTable(tableName: String, statement: Statement): Unit = {
        val sql = new StringBuilder()

        // Begin
        sql.append("create table " + tableName + " (")

        // Fields
        config.destination.schemaProperties.fields.forEach(field => {
            sql.append(field.name + " ")
            // If we have a semi-structured field and the type is defined, set it
            if((field.name.compareToIgnoreCase("_json") == 0 || field.name.compareToIgnoreCase("_xml") == 0) &&
                config.destination.database.snowflake.createSemiStructuredFieldAs != null)
            {
                sql.append(config.destination.database.snowflake.createSemiStructuredFieldAs.toLowerCase + ", ")
            }
            // Default the semi-structured field type if it does not exist to VARIANT
            else if((field.name.compareToIgnoreCase("_json") == 0 || field.name.compareToIgnoreCase("_xml") == 0) &&
                config.destination.database.snowflake.createSemiStructuredFieldAs == null)
            {
                sql.append("variant, ")
            }
            else if(field.`type`.compareTo("string") == 0)
                sql.append("varchar, ")
            else
                sql.append(field.`type` + ", ")
        })
        sql.setLength(sql.length - 2)

        // Keys?
        if(config.destination.database.snowflake.keyFields != null) {
            sql.append(", primary key (")
            config.destination.database.snowflake.keyFields.forEach(field => {
                sql.append(field + ", ")
            })
            sql.setLength(sql.length - 2)
            sql.append(")")
        }

        // End
        sql.append(");")

        statusUtil.info("processing", "Table does not exist, creating table: " + sql.mkString)
        statement.execute(sql.mkString)
    }

    private def asyncQuery(statement: Statement, sql: String): ResultSet = {
        val resultSet = statement.unwrap(classOf[SnowflakeStatement]).executeAsyncQuery(sql)

        var queryStatus = QueryStatus.RUNNING
        while ( {
            (queryStatus == QueryStatus.RUNNING) || (queryStatus == QueryStatus.RESUMING_WAREHOUSE)
        }) {
            Thread.sleep(500)
            queryStatus = resultSet.unwrap(classOf[SnowflakeResultSet]).getStatus
        }

        if (queryStatus == QueryStatus.FAILED_WITH_ERROR)
            throw new PipelineException("Query statement: " + sql + " failed with error code: " + queryStatus.getErrorCode.toString + ", error message: " + queryStatus.getErrorMessage)
        if (queryStatus != QueryStatus.SUCCESS)
            throw new PipelineException("Query statement: " + sql + " failed with unexpected status: " + queryStatus.toString)
        resultSet
    }

    private def sendNotification(): Unit = {
        val notification = Notification(
            config.name,
            jobContext.metadata.publisherToken,
            jobContext.pipelineToken,
            "snowflake",
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
        attributes.put("destination", "snowflake")
        attributes.put("schema", config.destination.database.schema)
        attributes.put("database", config.destination.database.dbName)
        attributes.put("table", config.destination.database.table)

        NotificationUtil.add(PipelineEnvironment.values.notifyTopicArn, jsonNotification, attributes.asScala.toMap)
        statusUtil.info("processing", "notification sent: " + jsonNotification)
    }
}
