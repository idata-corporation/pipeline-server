package net.idata.pipeline.util

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.common.model.{DatasetConfig, Notification, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{DatasetConfigIO, NotificationUtil, QueueUtil}
import net.idata.pipeline.model.{CDCMessage, DebeziumMessage}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.collection.JavaConverters._

class CDCMessageProcessor {
    private val logger: Logger = LoggerFactory.getLogger(classOf[CDCMessageProcessor])

    def process(cdcMessages: List[CDCMessage]): Unit = {
        val messages = cdcMessages.map(cdcMessage => {
            val decodedValue = new String(Base64.getDecoder.decode(cdcMessage.value), StandardCharsets.UTF_8)
            parseMessage(PipelineEnvironment.values.cdcDebeziumKafkaTopic, cdcMessage.topic, decodedValue)
        })
        processMessages(messages)
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
        }.asJava

        val after = {
            val afterNV = parseSubstring(message, "after=Struct{")
            if(afterNV != null)
                afterNV.split(",").map(_.split("=")).map(a=>(a(0), a(1))).toMap
            else
                null
        }.asJava

        val isInsert = before == null && after != null
        val isUpdate = before != null && after != null
        val isDelete = before != null && after == null

        val debeziumMessage = DebeziumMessage(topic, datasetName, isInsert, isUpdate, isDelete, before, after)
        if(PipelineEnvironment.values.cdcTopicArn != null)
            writeToCDCTopic(debeziumMessage)
        debeziumMessage
    }

    private def writeToCDCTopic(debeziumMessage: DebeziumMessage): Unit = {
        // Create the message attributes for the SNS filter policy
        val attributes = new java.util.HashMap[String, String]
        attributes.put("dataset", debeziumMessage.datasetName)
        attributes.put("cdcTopic", debeziumMessage.topic)

        val gson = new Gson
        NotificationUtil.addFifo(PipelineEnvironment.values.cdcTopicArn, gson.toJson(debeziumMessage), attributes.asScala.toMap)
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

    private def processMessages(messages: List[DebeziumMessage]): Unit = {
        val configs = messages.map(message => {
            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, message.datasetName)
            if (config == null)
                throw new PipelineException("CDC message error, dataset configuration: " + message.datasetName + " was not found in the NoSQL table: " + PipelineEnvironment.values.datasetTableName)
            config
        }).distinct

        val groupedByDestination = messages.map(message => {
            val config = configs.find(_.name.compareTo(message.datasetName) == 0).orNull
            if (config.destination.objectStore != null)
                ("objectStore", config, message)
            else if (config.destination.database != null && config.destination.database.snowflake != null)
                ("snowflake", config, message)
            else if (config.destination.database != null && config.destination.database.redshift != null)
                ("redshift", config, message)
            else
                throw new PipelineException("The proper destination is not set for the dataset " + config.name)
        }).groupBy(_._1)

        groupedByDestination.foreach { case (_, groupedMessages) =>
            processGroup(groupedMessages)
        }
    }

    private def processGroup(groupOfMessages: List[(String, DatasetConfig, DebeziumMessage)]): Unit = {
        try {
            val destination = groupOfMessages.head._1
            destination match {
                case "objectStore" =>
                    new ObjectStoreCDCUtil().process(groupOfMessages)
                case "snowflake" =>
                    // TODO
                case "redshift" =>
                    new RedshiftCDCUtil().process(groupOfMessages)
            }
        }
        catch {
            case e: Exception =>
                logger.info("Error writing CDC message to store: " + Throwables.getStackTraceAsString(e))
            // TODO - put this message in a queue or something to save it
        }
    }
}