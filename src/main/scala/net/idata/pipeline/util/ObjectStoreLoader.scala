package net.idata.pipeline.util

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

import com.google.gson.Gson
import net.idata.pipeline.model.{PipelineEnvironment, JobContext, Notification}
import net.idata.pipeline.util.aws.{GlueUtil, IcebergUtil}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.collection.JavaConverters._

class ObjectStoreLoader(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[ObjectStoreLoader])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the dataset into object store: " + config.name)

        val databaseName = config.destination.schemaProperties.dbName
        val tableName = config.name
        val objectStore = config.destination.objectStore

        // Move the file(s) to a unique path in the -temp bucket
        val tempLocation = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
        val files = DatasetMetadataUtil.getFiles(jobContext.metadata)
        files.foreach(fileUrl => {
            val tempFilename = jobContext.config.name + "." +  GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
            val tempUrl = tempLocation + tempFilename
            ObjectStoreUtil.copyBucketObject(
                ObjectStoreUtil.getBucket(fileUrl),
                ObjectStoreUtil.getKey(fileUrl),
                ObjectStoreUtil.getBucket(tempUrl),
                ObjectStoreUtil.getKey(tempUrl))
        })

        val destinationUrl = {
            if(objectStore.destinationBucketOverride != null)
                "s3://" + PipelineEnvironment.values.environment + "-" + objectStore.destinationBucketOverride + "/" + objectStore.prefixKey + "/" + config.name + "/"
            else
                "s3://" + PipelineEnvironment.values.environment + "-raw-plus/" + objectStore.prefixKey + "/" + config.name + "/"
        }
        val writeToTempUrl = {
            if(objectStore.writeToTemporaryLocation)
                "s3://" + PipelineEnvironment.values.environment + "-temp/delta/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
            else
                null
        }

        // Delete the existing data first?
        deleteBeforeWrite(destinationUrl)

        // Create a temporary Glue table for the incoming data
        val (tempDatabaseName, tempTableName) = createGlueTempTable(tempLocation)

        try {
            // Write the data to the object store destination
            writeData(databaseName, tableName, tempDatabaseName, tempTableName)

            // Write to temporary location?
            if(objectStore.writeToTemporaryLocation)
                ParquetUtil.convertCSVs(writeToTempUrl, config, jobContext.metadata)
        }
        finally {
            // Drop the temporary table
            GlueUtil.dropTable(tempDatabaseName, tempTableName)
        }

        sendNotification(destinationUrl, writeToTempUrl)
        statusUtil.info("end", "Process completed")
    }

    private def deleteBeforeWrite(destinationUrl: String): Unit = {
        if(config.destination.objectStore.deleteBeforeWrite) {
            if(config.destination.objectStore.useIceberg)
                IcebergUtil.deleteData(config.destination.schemaProperties.dbName, config.name)
            else {
                logger.info("'deleteBeforeWrite' flag is set, deleting data at: " + destinationUrl)
                ObjectStoreUtil.deleteFolder(ObjectStoreUtil.getBucket(destinationUrl), ObjectStoreUtil.getKey(destinationUrl))

                // Wait for S3 eventual consistency
                Thread.sleep(2000)
            }
        }
    }

    private def createGlueTempTable(tempLocation: String): (String, String) = {
        val tempDatabaseName = config.destination.schemaProperties.dbName + "_temp"
        val tempTableName = config.name + "_temp_" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString.replace("-", "")

        statusUtil.info("processing", "Creating a temporary Glue table: " + tempDatabaseName + "." + tempTableName)
        GlueUtil.createTable(
            tempDatabaseName,
            tempTableName,
            config.source.schemaProperties.fields.asScala.toList,
            null,
            tempLocation,
            fileFormat = "text",
            config.source.fileAttributes.csvAttributes.header,
            textFileDelimiter = config.source.fileAttributes.csvAttributes.delimiter
        )
        (tempDatabaseName, tempTableName)
    }

    private def writeData(databaseName: String, tableName: String, tempDatabaseName: String, tempTableName: String): Unit = {
        val sql = {
            // Iceberg dataset?
            if(config.destination.objectStore.useIceberg)
                IcebergUtil.buildSql(config, databaseName, tableName, tempDatabaseName, tempTableName)
            else {
                // Reorder the fields according to the partition, if any
                val fields = {
                    val partitionColumns = {
                        if(config.destination.objectStore.partitionBy != null)
                            config.destination.objectStore.partitionBy.asScala.toList
                        else
                            List[String]()
                    }
                    val destinationFields = config.destination.schemaProperties.fields.asScala.flatMap(field => {
                        // Remove the partition columns from the destination fields, we will add them at the end
                        if(partitionColumns.contains(field.name))
                            None
                        else
                            Some(field.name)
                    }).toList

                    // Partition columns must be at the end for Athena to work properly
                    destinationFields ::: partitionColumns
                }
                "INSERT INTO " + databaseName + "." + tableName + " SELECT " + fields.mkString(", ") + " FROM " + tempDatabaseName + "." + tempTableName
            }
        }

        logger.info("AthenUtil sql: " + sql)
        val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
        ObjectStoreSQLUtil.sql(databaseName, sql, outputPath)
    }

    private def sendNotification(destinationUrl: String, writeToTempUrl: String): Unit = {
        val notification = Notification(
            config.name,
            jobContext.metadata.publisherToken,
            jobContext.pipelineToken,
            "objectStore",
            config.destination.objectStore.prefixKey,
            destinationUrl,
            writeToTempUrl,
            null,
            null,
            null
        )
        val gson = new Gson
        val jsonNotification = gson.toJson(notification)
        logger.info("notification sent: " + jsonNotification)
        NotificationUtil.add(PipelineEnvironment.values.notifyTopicArn, jsonNotification)
    }
}