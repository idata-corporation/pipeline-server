package net.idata.pipeline.controller

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

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.model._
import net.idata.pipeline.util._
import org.slf4j.{Logger, LoggerFactory}

class FileNotifier {
    private val logger: Logger = LoggerFactory.getLogger(classOf[FileNotifier])
    private val statusUtil = new StatusUtil().init(PipelineEnvironment.values.datasetStatusTableName, this.getClass.getSimpleName)

    def process(bucket: String, key: String): JobContext = {
        logger.info("Processing queue message, bucket: " + bucket + ", key: " + key)
        statusUtil.setFilename(bucket + "/" + key)

        try {
            // Generate a UUID to track the dataset through the pipeline
            val pipelineToken = GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString
            statusUtil.setPipelineToken(pipelineToken)

            val metadata = DatasetMetadataUtil.read(bucket, key)
            statusUtil.setFilename(metadata)
            statusUtil.setPublisherToken(metadata.publisherToken)

            // Save the metadata in DynamoDb
            val gson = new Gson
            val jsonMetadata = gson.toJson(metadata)
            NoSQLDbUtil.setItemNameValue(PipelineEnvironment.values.archivedMetadataTableName, "pipeline_token", pipelineToken, "metadata", jsonMetadata)

            statusUtil.info("begin", "Data received, bucket: " + bucket + ", key: " + key)

            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, metadata.dataset)
            if(config == null)
                throw new PipelineException("Dataset: " + metadata.dataset + " is not configured in the NoSQL database")
            val fileSize = getFileSize(bucket, key, metadata)

            statusUtil.info("end", "Process completed successfully")

            JobContext(bucket, key, pipelineToken, metadata, fileSize, config, INITIALIZED, null, statusUtil)
        } catch {
            case e: Exception =>
                statusUtil.error("end", "Process completed, error: " + Throwables.getStackTraceAsString(e))
                throw new PipelineException("FileNotifier error: " +Throwables.getStackTraceAsString(e))
        }
    }

    private def getFileSize(bucket: String, key: String, metadata: DatasetMetadata): Long = {
        // Get the file size
        val objectMetadata = ObjectStoreUtil.getObjectMetatadata(bucket, key)
        val objectSize = {
            // Bulk file ingestion?
            if(metadata.dataFilePath != null) {
                val summaries = ObjectStoreUtil.listSummaries(ObjectStoreUtil.getBucket(metadata.dataFilePath),
                    ObjectStoreUtil.getKey(metadata.dataFilePath))
                summaries.map(_.getSize).sum
            }
            else
                objectMetadata.getContentLength
        }
        statusUtil.info("processing", "Total file size: " + objectSize.toString)
        objectSize
    }
}