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
import net.idata.pipeline.common.model._
import net.idata.pipeline.common.model.spark.SparkRuntime
import net.idata.pipeline.common.util._
import net.idata.pipeline.model.{INITIALIZED, JobContext}
import net.idata.pipeline.util.{DataUtil, DatasetMetadataUtil}
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

            // Read the data into memory, if necessary
            val data = {
                // If we're transforming to parquet (object store) using a spark cluster and there is no data quality, transformation,
                // or database load, don't load the data. The spark job will read and transform the data
                if(config.destination.objectStore != null &&
                    config.destination.objectStore.useSparkCluster &&
                    config.dataQuality == null &&
                    config.transformation == null &&
                    config.destination.database == null)
                {
                    null
                }
                else
                    DataUtil.read(bucket, key, config, metadata, statusUtil)
            }
            if(data != null)
                statusUtil.info("processing", "Total file size: " + data.size.toString)

            val sparkRuntime = {
                if(config.destination.objectStore != null && config.destination.objectStore.useSparkCluster)
                    getSparkRuntime(bucket, key, pipelineToken, metadata.publisherToken, metadata, config)
                else
                    null
            }

            statusUtil.info("end", "Process completed successfully")
            JobContext(pipelineToken, metadata, data, config, INITIALIZED, null, statusUtil, sparkRuntime, null)
        } catch {
            case e: Exception =>
                statusUtil.error("end", "Process completed, error: " + Throwables.getStackTraceAsString(e))
                throw new PipelineException("FileNotifier error: " +Throwables.getStackTraceAsString(e))
        }
    }

    private def getSparkRuntime(bucket: String, key: String, pipelineToken: String, publisherToken: String, metadata: DatasetMetadata, config: DatasetConfig): SparkRuntime = {
        val environment = PipelineEnvironment.values.environment

        val sourceTransformUrl = {
            if(metadata.dataFilePath != null) {
                if(! metadata.dataFilePath.endsWith("/"))
                    metadata.dataFilePath + "/"
                else
                    metadata.dataFilePath
            }
            else {
                getDataFileUrlFromMetadataLocation(bucket, key, metadata.dataFileName)
            }
        }

        val destinationTransformUrl = {
            if(config.destination.objectStore.destinationBucketOverride != null)
                "s3://" + config.destination.objectStore.destinationBucketOverride + "/" + config.destination.objectStore.prefixKey + "/" + config.name + "/parquet"
            else
                "s3://" + environment + "-" + "-raw-plus" + "/" + config.destination.objectStore.prefixKey + "/" + config.name + "/parquet"
        }

        spark.SparkRuntime(
            config.name,
            publisherToken,
            pipelineToken,
            metadata,
            sourceTransformUrl,
            destinationTransformUrl,
            config.destination.objectStore.useIceberg,
            PipelineEnvironment.values
        )
    }

    private def getDataFileUrlFromMetadataLocation(bucket: String, key: String, datafileName: String): String = {
        // The datafile must be dropped into the same location as the metadata file
        val endIndex = key.lastIndexOf("/")
        if (endIndex == -1)
            throw new PipelineException("Internal error: invalid metadata S3 key missing final /")
        "s3://" + bucket + "/" + key.substring(0, endIndex) + "/" + datafileName
    }
}