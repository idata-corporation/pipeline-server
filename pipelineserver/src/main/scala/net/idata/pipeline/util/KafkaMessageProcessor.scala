package net.idata.pipeline.util

import com.google.common.base.Throwables
import net.idata.pipeline.common.model.{DatasetConfig, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{DatasetConfigIO, GuidV5, ObjectStoreSQLUtil}
import net.idata.pipeline.model.{CDCMessage, DebeziumMessage, JobContext}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64

class KafkaMessageProcessor() {
    private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaMessageProcessor])

    def process(cdcMessage: CDCMessage): Unit = {
        val decodedValue = new String(Base64.getDecoder.decode(cdcMessage.value), StandardCharsets.UTF_8)

        val message = parseMessage(PipelineEnvironment.values.cdcDebeziumKafkaTopic, cdcMessage.topic, decodedValue)
        logger.info("CDC message: " + message.toString)
        processMessage(message)
    }

    private def parseMessage(configuredTopic: String, topic: String, message: String): DebeziumMessage = {
        val datasetName = {
            val newTopic = topic.replace(configuredTopic, "")
            newTopic.substring(1).replace(".", "_")
        }

        val before = {
            val beforeNV = parseSubstring(message, "before=Struct{")
            if(beforeNV != null)
                beforeNV.split(",").map(_.split("=")).map(a=>(a(0), a(1))).toMap
            else
                null
        }

        val after = {
            val afterNV = parseSubstring(message, "after=Struct{")
            if(afterNV != null)
                afterNV.split(",").map(_.split("=")).map(a=>(a(0), a(1))).toMap
            else
                null
        }

        val isInsert = (before == null && after != null)
        val isUpdate = (before != null && after != null)
        val isdelete = (before != null && after == null)

        logger.info("before: " + before + ", after: " + after)
        DebeziumMessage(topic, datasetName, isInsert, isUpdate, isdelete, before, after)
    }

    private def parseSubstring(initialValue: String, searchFor: String): String = {
        if(initialValue.contains(searchFor)) {
            val found = initialValue.substring(initialValue.indexOf(searchFor))
            if(found == null)
                null
            else
                found.substring(searchFor.length, found.indexOf("}"))
        }
        else
            null
    }

    private def processMessage(message: DebeziumMessage): Unit = {
        try {
            // Read the dataset configuration
            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, message.datasetName)
            if(config == null)
                throw new PipelineException("CDC message error, dataset configuration: " + message.datasetName + " was not found in the NoSQL table: " + PipelineEnvironment.values.datasetTableName)
            if(config.destination.objectStore == null && config.destination.database == null)
                throw new PipelineException("CDC message error, only objectStore and databases are supported for the destination of a CDC message")
            if(config.destination.objectStore != null && !config.destination.objectStore.useIceberg)
                throw new PipelineException("CDC message error, when the destination is objectStore, only Iceberg tables are supported for the destination of a CDC message")

            val sql = {
                if(message.isInsert)
                    GenerateSQLUtil.insert(config, message)
                else if(message.isUpdate)
                    GenerateSQLUtil.update(config, message)
                else
                    GenerateSQLUtil.delete(config, message)
            }
            logger.info("CDC SQL created: " + sql)
            writeToStore(config, sql)
        }
        catch {
            case e: Exception =>
                logger.info("Error writing CDC message to store: " + Throwables.getStackTraceAsString(e))
            // TODO - put this message in a queue or something to save it
        }
    }

    private def writeToStore(config: DatasetConfig, sql: String): Unit = {
        // Write to object store?
        if(config.destination.objectStore != null) {
            val databaseName = config.destination.schemaProperties.dbName
            val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
            ObjectStoreSQLUtil.sql(databaseName, sql, outputPath)
        }
        else if(config.destination.database != null && config.destination.database.snowflake != null) {
        }
        else if(config.destination.database != null && config.destination.database.redshift != null) {
            new RedshiftLoader(JobContext(null, null, null, null, null, null, null)).executeSQL(sql)
        }
        else
            throw new PipelineException("Destinations supported for CDC are object store, Redshift and Snowflake")
    }
}