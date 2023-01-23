package net.idata.pipeline.util.aws

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

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

import net.idata.pipeline.model.{DatasetConfig, PipelineEnvironment}
import net.idata.pipeline.util.{GuidV5, ObjectStoreSQLUtil}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.collection.JavaConverters._

object IcebergUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def createTable(config: DatasetConfig, locationUrl: String): Unit = {
        val databaseName = config.destination.schemaProperties.dbName
        val tableName = config.name
        val fields = config.destination.schemaProperties.fields.asScala
        val partitionBy = {
            if(config.destination.objectStore.partitionBy != null)
                config.destination.objectStore.partitionBy.asScala.toList
            else
                null
        }

        // Create the table using sql and Athena
        val sql = new StringBuilder()

        sql.append("CREATE TABLE ")
        sql.append(databaseName + "." + tableName + " (")

        // Columns
        fields.foreach(field => {
            sql.append(field.name + " " + field.`type` + ", ")
        })
        sql.setLength(sql.length - 2)
        sql.append(") ")

        // Partition by
        if(partitionBy != null) {
            sql.append("PARTITIONED BY (")
            partitionBy.foreach(field => {
                sql.append(field + ", ")
            })
        }
        sql.setLength(sql.length - 2)
        sql.append(") ")

        // Location
        sql.append("LOCATION " + "'" + locationUrl + "' ")

        // Table properties
        sql.append("TBLPROPERTIES ('table_type' = 'ICEBERG')")

        logger.info("AthenUtil sql: " + sql)
        val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
        ObjectStoreSQLUtil.sql(databaseName, sql.toString, outputPath)
    }

    def doesTableExist(databaseName: String, tableName: String): Boolean = {
        val sql = "DESCRIBE " + databaseName + "." + tableName
        logger.info("AthenUtil sql: " + sql)
        val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
        try {
            ObjectStoreSQLUtil.sql(databaseName, sql, outputPath)
            true
        }
        catch {
            case e: Exception =>
                false
        }
    }

    def deleteData(databaseName: String, tableName: String): Unit = {
        val sql = "DELETE FROM " + databaseName + "." + tableName
        logger.info("AthenUtil sql: " + sql)
        val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
        try {
            ObjectStoreSQLUtil.sql(databaseName, sql, outputPath)
        }
        catch {
            case e: Exception => logger.info("No data exists in the table")
                // Ignore the exception, it means the table has not ever been written to
        }
    }

    def buildSql(config: DatasetConfig, databaseName: String, tableName: String, tempDatabaseName: String, tempTableName: String): String = {
        // If we have key fields, build a MERGE INTO, otherwise INSERT INTO
        if(config.destination.objectStore.keyFields != null)
            buildMergeIntoSql(config, databaseName, tableName, tempDatabaseName, tempTableName)
        else
            buildInsertIntoSql(config, databaseName, tableName, tempDatabaseName, tempTableName)
    }

    private def buildInsertIntoSql(config: DatasetConfig, databaseName: String, tableName: String, tempDatabaseName: String, tempTableName: String): String = {
        val sql = new StringBuilder()
        sql.append("INSERT INTO " + databaseName + "." + tableName + " SELECT ")

        val insertFields = config.destination.schemaProperties.fields.asScala.map(field => {
            field.name
        }).mkString(", ")
        sql.append(insertFields)

        sql.append(" FROM " + tempDatabaseName + "." + tempTableName)
        sql.toString
    }

    private def buildMergeIntoSql(config: DatasetConfig, databaseName: String, tableName: String, tempDatabaseName: String, tempTableName: String): String = {
        val sql = new StringBuilder()

        sql.append("MERGE INTO " + databaseName + "." + tableName + " d USING " + tempDatabaseName + "." + tempTableName + " s")
        sql.append(" ON (")

        val condition = config.destination.objectStore.keyFields.asScala.map(field => {
            "d." + field + " = s." + field
        }).toList.mkString(" AND ")
        sql.append(condition)
        sql.append(") ")

        sql.append("WHEN MATCHED THEN UPDATE SET ")
        val setFields = config.destination.schemaProperties.fields.asScala.map(field => {
            field.name + " = s." + field.name
        }).mkString(", ")
        sql.append(setFields)

        sql.append(" WHEN NOT MATCHED THEN INSERT (")
        val insertFields = config.destination.schemaProperties.fields.asScala.map(field => {
            field.name
        }).mkString(", ")
        sql.append(insertFields)
        sql.append(") ")

        sql.append("VALUES (")
        val insertValues = config.destination.schemaProperties.fields.asScala.map(field => {
            "s." + field.name
        }).mkString(", ")
        sql.append(insertValues)
        sql.append(")")

        sql.toString
    }
}
