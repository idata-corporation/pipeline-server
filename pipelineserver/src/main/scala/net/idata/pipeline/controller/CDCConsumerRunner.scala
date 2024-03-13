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
import net.idata.pipeline.common.util.QueueUtil
import net.idata.pipeline.model.cdc.CDCMessage
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import java.util.{Base64, Properties}
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
            val messageList = new ListBuffer[CDCMessage]()
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(3000)
                val messagesReceived: Boolean = records.count() > 0
                if(messagesReceived)
                    logger.info("Kafka topic messages received: " + records.count().toString)
                val before = System.currentTimeMillis
                records.asScala.foreach(consumerRecord => {
                    logger.info("Message received, topic: " + consumerRecord.topic() + ", key: " + consumerRecord.key() + ", " + consumerRecord.value())

                    if(shouldProcess(consumerRecord.value())) {
                        val encodedValue = Base64.getEncoder.encodeToString(consumerRecord.value().getBytes(StandardCharsets.UTF_8))

                        // Add the message to the list
                        val message = CDCMessage(consumerRecord.topic(), encodedValue)
                        messageList += message
                    }
                })
                if(messagesReceived) {
                    // Add the message list to the queue
                    val gson = new Gson()
                    val json = gson.toJson(messageList.asJava)
                    QueueUtil.addFifo(PipelineEnvironment.values.cdcMesssageQueue, json)
                    messageList.clear()
                    val totalTime=System.currentTimeMillis-before
                    logger.info("Milliseconds to add messages to FIFO queue: " + totalTime.toString)
                }
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline Kafka error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def shouldProcess(value: String): Boolean = {
        if(value == null)
            // SQL deletes send an extra message that has a null value
            false
        else if(value.contains("tableChanges="))
            // Do not process database changes
            false
        else
            true
    }
}
