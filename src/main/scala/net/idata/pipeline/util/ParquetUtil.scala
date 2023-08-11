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

import net.idata.pipeline.model.{DatasetConfig, JobContext, PipelineEnvironment}
import net.idata.pipeline.util.aws.GlueUtil

import java.time.Instant
import scala.collection.JavaConverters._

object ParquetUtil {
    def convertCSVs(jobContext: JobContext, destinationUrl: String, config: DatasetConfig): Unit = {
        val data = jobContext.data

        // Prepare Glue database and table names
        val tempDatabaseName = config.destination.schemaProperties.dbName + "_temp"
        val sourceTempTableName = config.name + "_temp_" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString.replace("-", "")
        Thread.sleep(100)
        val destTempTableName = config.name + "_temp_" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString.replace("-", "")

        // Move the incoming data to a unique path in the -temp bucket
        val tempLocation = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
        val tempFilename = config.name + "." +  GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
        val tempUrl = tempLocation + tempFilename
        ObjectStoreUtil.writeBucketObject(
            ObjectStoreUtil.getBucket(tempUrl),
            ObjectStoreUtil.getKey(tempUrl),
            data.rows.mkString("\n"))

        // Create a Glue temp table for the incoming data (text format)
        GlueUtil.createTable(
            tempDatabaseName,
            sourceTempTableName,
            data.headerWithSchema,
            null,
            tempLocation,
            fileFormat = "text",
            textFileDelimiter = config.source.fileAttributes.csvAttributes.delimiter
        )

        // Create a Glue temp table for the destination stage area (parquet format)
        GlueUtil.createTable(
            tempDatabaseName,
            destTempTableName,
            config.destination.schemaProperties.fields.asScala.toList,
            null,
            destinationUrl
        )

        try {
            // Write the parquet data to the Snowflake stage area
            val sql = "INSERT INTO " + tempDatabaseName + "." + destTempTableName + " SELECT " + config.destination.schemaProperties.fields.asScala.map(_.name).mkString(", ") + " FROM " + tempDatabaseName + "." + sourceTempTableName
            jobContext.statusUtil.info("processing", "AthenUtil sql: " + sql)
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
