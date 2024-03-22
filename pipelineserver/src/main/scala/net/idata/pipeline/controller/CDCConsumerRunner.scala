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
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{NotificationUtil, QueueUtil}
import net.idata.pipeline.model.{CDCMessage, DebeziumMessage}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CDCConsumerRunner extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[CDCConsumerRunner])

    def run(): Unit = {
        try {
            val properties = new Properties()
            properties.put("bootstrap.servers", PipelineEnvironment.values.kafkaBootstrapServer)
            properties.put("group.id", PipelineEnvironment.values.kafkaGroupId)
            properties.put("key.deserializer", classOf[StringDeserializer])
            properties.put("value.deserializer", classOf[StringDeserializer])

            val consumer: Consumer[String, String] = new KafkaConsumer[String, String](properties)
            val pattern = Pattern.compile(PipelineEnvironment.values.cdcDebeziumKafkaTopic + ".*")
            consumer.subscribe(pattern)
            val messageList = new ListBuffer[DebeziumMessage]()
            var messageListSize = 0
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(3000)
                val messagesReceived: Boolean = records.count() > 0
                if(messagesReceived)
                    logger.info("Kafka topic messages received: " + records.count().toString)
                records.asScala.foreach(consumerRecord => {
                    logger.info("Message received, topic: " + consumerRecord.topic() + ", key: " + consumerRecord.key() + ", value: " + consumerRecord.value())

                    val debeziumMessage = parseMessage(PipelineEnvironment.values.cdcDebeziumKafkaTopic, consumerRecord.topic(), consumerRecord.value())
                    if(debeziumMessage != null) {
                        // Determine the size of the message
                        val gson = new Gson()
                        val debeziumMessageSize = gson.toJson(debeziumMessage).length

                        // The message list cannot exceed 255Kb, SNS limit is 256Kb
                        if(debeziumMessageSize + messageListSize >= (255*1024)) {
                            sendMessageList(messageList)
                            messageList.clear()
                            messageList += debeziumMessage
                        }
                        else
                            messageList += debeziumMessage
                        messageListSize = messageListSize + debeziumMessageSize
                    }
                })

                if(messageList.nonEmpty) {
                    sendMessageList(messageList)
                    messageList.clear()
                }
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline Kafka error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def parseMessage(configuredTopic: String, topic: String, json: String): DebeziumMessage = {
        val gson = new Gson()
        val message =  gson.fromJson(json, classOf[CDCMessage])
        if(shouldProcess(message)) {
            val datasetName = {
                val newTopic = topic.replace(configuredTopic, "")
                newTopic.substring(1).replace(".", "_")
            }

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

            DebeziumMessage(
                topic,
                datasetName,
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

    private def shouldProcess(message: CDCMessage): Boolean = {
        if(message.tableChanges != null) {
            logger.info("CDC message, table change received, ignore")
            false
        }
        else if(message.before == null && message.after == null) {
            logger.info("CDC message, before and after are null, ignore")
            false
        }
        else
            true
    }

    private def sendMessageList(messageList: ListBuffer[DebeziumMessage]): Unit = {
        // Add the message to the CDC Message Queue?
        if(PipelineEnvironment.values.cdcMesssageQueue != null) {
            val gson = new Gson()
            val json = gson.toJson(messageList.asJava)
            QueueUtil.addFifo(PipelineEnvironment.values.cdcMesssageQueue, json)
        }

        // Send notifications?
        if(PipelineEnvironment.values.cdcTopicArn != null) {
            messageList.toList.foreach(debeziumMessage => {
                // Create the message attributes for the SNS filter policy
                val attributes = new java.util.HashMap[String, String]
                attributes.put("cdcTopic", debeziumMessage.topic)
                attributes.put("dataset", debeziumMessage.datasetName)
                attributes.put("schema", debeziumMessage.schemaName)
                attributes.put("database", debeziumMessage.databaseName)
                attributes.put("table", debeziumMessage.tableName)

                val gson = new Gson
                logger.info("Sending message for table: " + debeziumMessage.tableName + " to topic: " + PipelineEnvironment.values.cdcTopicArn)
                NotificationUtil.addFifo(PipelineEnvironment.values.cdcTopicArn, gson.toJson(debeziumMessage), attributes.asScala.toMap)
            })
        }
    }
}
