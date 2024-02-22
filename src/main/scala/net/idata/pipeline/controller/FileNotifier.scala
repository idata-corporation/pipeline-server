package net.idata.pipeline.controller

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

            val metadata = new DatasetMetadataUtil(statusUtil).read(bucket, key)
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

            // Read the data into memory
            val data = DataUtil.read(bucket, key, config, metadata, statusUtil)
            statusUtil.info("processing", "Total file size: " + data.size.toString)

            statusUtil.info("end", "Process completed successfully")

            JobContext(bucket, key, pipelineToken, metadata, data, config, INITIALIZED, null, statusUtil)
        } catch {
            case e: Exception =>
                statusUtil.error("end", "Process completed, error: " + Throwables.getStackTraceAsString(e))
                throw new PipelineException("FileNotifier error: " +Throwables.getStackTraceAsString(e))
        }
    }
}