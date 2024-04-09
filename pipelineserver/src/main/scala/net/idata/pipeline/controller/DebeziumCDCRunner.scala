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
import com.google.gson.{Gson, GsonBuilder}
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.NotificationUtil
import net.idata.pipeline.model.{CDCMessage, DebeziumMessage}
import net.idata.pipeline.util.CDCMessageProcessor
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DebeziumCDCRunner extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DebeziumCDCRunner])

    def run(): Unit = {
        try {
            val properties = new Properties()
            properties.put("bootstrap.servers", PipelineEnvironment.values.cdcConfig.debeziumConfig.kafkaBootstrapServer)
            properties.put("group.id", PipelineEnvironment.values.cdcConfig.debeziumConfig.kafkaGroupId)
            properties.put("key.deserializer", classOf[StringDeserializer])
            properties.put("value.deserializer", classOf[StringDeserializer])

            val consumer: Consumer[String, String] = new KafkaConsumer[String, String](properties)
            val pattern = Pattern.compile(PipelineEnvironment.values.cdcConfig.debeziumConfig.kafkaTopic + ".*")
            consumer.subscribe(pattern)
            val messageList = new ListBuffer[CDCMessage]()
            var messageListSize = 0
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(PipelineEnvironment.values.cdcConfig.debeziumConfig.kafkaTopicPollingInterval)
                val messagesReceived: Boolean = records.count() > 0
                if (messagesReceived)
                    logger.info("Kafka topic messages received: " + records.count().toString)
                records.asScala.foreach(consumerRecord => {
                    //logger.info("Message received, topic: " + consumerRecord.topic() + ", key: " + consumerRecord.key() + ", value: " + consumerRecord.value())

                    val cdcMessage = parseMessage(consumerRecord.topic(), consumerRecord.value())
                    if (cdcMessage != null) {
                        // Determine the size of the message
                        val gson = new Gson()
                        val size = gson.toJson(cdcMessage).length

                        // The message list cannot exceed 255Kb, SNS limit is 256Kb
                        if (size + messageListSize >= (255 * 1024)) {
                            processMessages(messageList.toList)
                            messageList.clear()
                            messageList += cdcMessage
                        }
                        else
                            messageList += cdcMessage
                        messageListSize = messageListSize + size
                    }
                })

                if (messageList.nonEmpty) {
                    processMessages(messageList.toList)
                    messageList.clear()
                }
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline DebeziumCDCRunner error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def parseMessage(topic: String, json: String): CDCMessage = {
        val gson = new Gson()
        val message = gson.fromJson(json, classOf[DebeziumMessage])
        if (shouldProcess(message)) {
            val before = {
                if (message.before != null)
                    message.before
                else
                    null
            }
            val after = {
                if (message.after != null)
                    message.after
                else
                    null
            }

            val isInsert = before == null && after != null
            val isUpdate = before != null && after != null
            val isDelete = before != null && after == null

            CDCMessage(
                topic,
                message.source.schema,
                message.source.db,
                message.source.table,
                isInsert,
                isUpdate,
                isDelete,
                before,
                after)
        }
        else
            null
    }

    private def shouldProcess(message: DebeziumMessage): Boolean = {
        if (message.tableChanges != null) {
            logger.info("CDC message, table change received, ignore")
            false
        }
        else if (message.before == null && message.after == null) {
            logger.info("CDC message, before and after are null, ignore")
            false
        }
        else
            true
    }

    private def processMessages(messages: List[CDCMessage]): Unit = {
        if (PipelineEnvironment.values.cdcConfig.publishMessages)
            publishMessages(messages)

        if (PipelineEnvironment.values.cdcConfig.processMessages) {
            val thread = new Thread(new CDCMessageProcessor(messages))
            thread.start()
        }
    }

    private def publishMessages(messages: List[CDCMessage]): Unit = {
        val gson = new GsonBuilder().disableHtmlEscaping().create()

        // Send notifications
        messages.foreach(message => {
            // Create the message attributes for the SNS filter policy
            val attributes = new java.util.HashMap[String, String]
            attributes.put("cdcTopic", message.topic)
            attributes.put("schema", message.schemaName)
            attributes.put("database", message.databaseName)
            attributes.put("table", message.tableName)

            logger.info("Sending message for table: " + message.tableName + " to topic: " + PipelineEnvironment.values.cdcConfig.publishSNSTopicArn)
            NotificationUtil.addFifo(PipelineEnvironment.values.cdcConfig.publishSNSTopicArn, gson.toJson(message), attributes.asScala.toMap)
        })
    }
}