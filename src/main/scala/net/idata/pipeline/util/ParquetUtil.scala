package net.idata.pipeline.util

import net.idata.pipeline.model.{DatasetConfig, DatasetMetadata, PipelineEnvironment}
import net.idata.pipeline.util.aws.GlueUtil
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.collection.JavaConverters._
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

object ParquetUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def convertCSVs(destinationUrl: String, config: DatasetConfig, metadata: DatasetMetadata): Unit = {
        // Prepare Glue database and table names
        val tempDatabaseName = config.destination.schemaProperties.dbName + "_temp"
        val sourceTempTableName = config.name + "_temp_" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString.replace("-", "")
        Thread.sleep(100)
        val destTempTableName = config.name + "_temp_" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString.replace("-", "")

        // Move the incoming file(s) to a unique path in the -temp bucket
        val tempLocation = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
        val files = DatasetMetadataUtil.getFiles(metadata)
        files.foreach(fileUrl => {
            val tempFilename = config.name + "." +  GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
            val tempUrl = tempLocation + tempFilename
            ObjectStoreUtil.copyBucketObject(
                ObjectStoreUtil.getBucket(fileUrl),
                ObjectStoreUtil.getKey(fileUrl),
                ObjectStoreUtil.getBucket(tempUrl),
                ObjectStoreUtil.getKey(tempUrl))
        })

        // Create a Glue temp table for the source data (text format)
        GlueUtil.createTable(
            tempDatabaseName,
            sourceTempTableName,
            config.source.schemaProperties.fields.asScala.toList,
            null,
            tempLocation,
            fileFormat = "text",
            config.source.fileAttributes.csvAttributes.header,
            textFileDelimiter = config.source.fileAttributes.csvAttributes.delimiter
        )

        // Create a Glue temp table for the destination stage area (parquet format)
        GlueUtil.createTable(
            tempDatabaseName,
            destTempTableName,
            config.destination.schemaProperties.fields.asScala.toList,  // NOTE: the destination table has only the destination schema fields
            null,
            destinationUrl
        )

        try {
            // Write the parquet data to the Snowflake stage area
            val sql = "INSERT INTO " + tempDatabaseName + "." + destTempTableName + " SELECT " + config.destination.schemaProperties.fields.asScala.map(_.name).mkString(", ") + " FROM " + tempDatabaseName + "." + sourceTempTableName
            logger.info("AthenUtil sql: " + sql)
            val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
            ObjectStoreSQLUtil.sql(tempDatabaseName, sql, outputPath)
        }
        finally {
            // Drop the temporary tables
            GlueUtil.dropTable(tempDatabaseName, sourceTempTableName)
            GlueUtil.dropTable(tempDatabaseName, destTempTableName)
        }
    }
}
