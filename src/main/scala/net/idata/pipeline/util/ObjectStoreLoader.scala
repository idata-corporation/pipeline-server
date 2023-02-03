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
import net.idata.pipeline.model.{JobContext, Notification, PipelineEnvironment}
import net.idata.pipeline.util.aws.{GlueUtil, IcebergUtil}

import java.time.Instant
import scala.collection.JavaConverters._

class ObjectStoreLoader(jobContext: JobContext) {
    private val config = jobContext.config
    private val objectStore = config.destination.objectStore
    private val statusUtil = jobContext.statusUtil

    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)

        statusUtil.info("begin", "Loading the dataset into object store: " + config.name)

        if(config.source.fileAttributes.unstructuredAttributes != null)
            processUnstructuredData()
        else
            processStructuredData()

        statusUtil.info("end", "Process completed")
    }

    private def processStructuredData(): Unit = {
        val databaseName = config.destination.schemaProperties.dbName
        val tableName = config.name

        // Move the data to a unique path in the -temp bucket
        val tempLocation = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
        val tempFilename = jobContext.config.name + "." +  GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
        val tempUrl = tempLocation + tempFilename
        val data = jobContext.data.rows.mkString("\n")
        ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(tempUrl), ObjectStoreUtil.getKey(tempUrl), data)

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
                ParquetUtil.convertCSVs(jobContext, writeToTempUrl, config)
        }
        finally {
            // Drop the temporary table
            GlueUtil.dropTable(tempDatabaseName, tempTableName)
        }

        sendNotification(destinationUrl, writeToTempUrl)
    }

    private def processUnstructuredData(): Unit = {
        // Simply copy the original source file(s) to the destination
        //

        val baseDestinationUrl = {
            if(objectStore.destinationBucketOverride != null)
                "s3://" + PipelineEnvironment.values.environment + "-" + objectStore.destinationBucketOverride + "/" + objectStore.prefixKey + "/" + config.name + "/"
            else
                "s3://" + PipelineEnvironment.values.environment + "-raw-plus/" + objectStore.prefixKey + "/" + config.name + "/"
        }
        val baseTempUrl = {
            if(objectStore.writeToTemporaryLocation)
                "s3://" + PipelineEnvironment.values.environment + "-temp/delta/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
            else
                null
        }

        // Delete the existing data first?
        deleteBeforeWrite(baseDestinationUrl)

        val files = new DatasetMetadataUtil(statusUtil).getFiles(jobContext.metadata)
        files.foreach(fileUrl => {
            val filename = {
                if(config.source.fileAttributes.unstructuredAttributes.preserveFilename)
                    fileUrl.substring(fileUrl.lastIndexOf('/') + 1)
                else
                    GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "." + config.source.fileAttributes.unstructuredAttributes.fileExtension
            }
            val destinationUrl = baseDestinationUrl + filename

            statusUtil.info("processing", "Copying: " + fileUrl + " to: " + destinationUrl)
            ObjectStoreUtil.copyBucketObject(
                ObjectStoreUtil.getBucket(fileUrl),
                ObjectStoreUtil.getKey(fileUrl),
                ObjectStoreUtil.getBucket(destinationUrl),
                ObjectStoreUtil.getKey(destinationUrl))

            // Write to temporary location
            val tempUrl = baseTempUrl + filename
            if(objectStore.writeToTemporaryLocation) {
                ObjectStoreUtil.copyBucketObject(
                    ObjectStoreUtil.getBucket(fileUrl),
                    ObjectStoreUtil.getKey(fileUrl),
                    ObjectStoreUtil.getBucket(tempUrl),
                    ObjectStoreUtil.getKey(tempUrl))
            }
        })

        sendNotification(baseDestinationUrl, baseTempUrl)
    }

    private def deleteBeforeWrite(destinationUrl: String): Unit = {
        if(config.destination.objectStore.deleteBeforeWrite) {
            if(config.destination.objectStore.useIceberg)
                IcebergUtil.deleteData(config.destination.schemaProperties.dbName, config.name)
            else {
                statusUtil.info("processing", "'deleteBeforeWrite' flag is set, deleting data at: " + destinationUrl)
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
            config.destination.schemaProperties.fields.asScala.toList,
            null,
            tempLocation,
            fileFormat = "text",
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

        statusUtil.info("processing", "AthenUtil sql: " + sql)
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

        // Create the message attributes for the SNS filter policy
        val attributes = new java.util.HashMap[String, String]
        attributes.put("dataset", config.name)
        attributes.put("prefixKey", config.destination.objectStore.prefixKey)
        attributes.put("destination", "objectStore")

        NotificationUtil.add(PipelineEnvironment.values.notifyTopicArn, jsonNotification, attributes.asScala.toMap)
        statusUtil.info("processing", "notification sent: " + jsonNotification)
    }
}