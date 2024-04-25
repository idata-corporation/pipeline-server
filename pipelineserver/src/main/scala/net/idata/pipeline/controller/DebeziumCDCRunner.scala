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
import net.idata.pipeline.model.{CDCMessage, DebeziumMessage}
import net.idata.pipeline.util.{CDCMessageProcessor, CDCMessagePublisher, CDCUtil}
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

            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(PipelineEnvironment.values.cdcConfig.debeziumConfig.kafkaTopicPollingInterval)
                val messagesReceived: Boolean = records.count() > 0
                if (messagesReceived)
                    logger.info("Kafka topic messages received: " + records.count().toString)

                val cdcMessages = records.asScala.map(consumerRecord => {
                    parseMessage(consumerRecord.value())
                }).toList

                if(cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.publishMessages) {
                    val thread = new Thread(new CDCMessagePublisher(cdcMessages))
                    thread.start()
                }

                if(cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.processMessages) {
                    val thread = new Thread(new CDCMessageProcessor(cdcMessages))
                    thread.start()
                }
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline DebeziumCDCRunner error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def parseMessage(json: String): CDCMessage = {
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
                message.source.db,
                message.source.schema,
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
}