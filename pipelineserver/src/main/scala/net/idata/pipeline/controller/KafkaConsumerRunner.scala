package net.idata.pipeline.controller

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.QueueUtil
import net.idata.pipeline.model.CDCMessage
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import java.util.{Base64, Properties}
import scala.collection.JavaConverters._

class KafkaConsumerRunner() extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaConsumerRunner])

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
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(2000)
                val logTime: Boolean = records.count() > 0
                if(logTime)
                    logger.info("Kafka topic messages received: " + records.count().toString)
                val before = System.currentTimeMillis;
                records.asScala.foreach(consumerRecord => {
                    logger.info("Message received, topic: " + consumerRecord.topic() + ", key: " + consumerRecord.key() + ", " + consumerRecord.value())

                    // If the value is null, ignore the message.  SQL deletes send an extra message that has a null value
                    if(consumerRecord.value() != null) {
                        val encodedValue = Base64.getEncoder.encodeToString(consumerRecord.value().getBytes(StandardCharsets.UTF_8))

                        // Add the message to the queue
                        val message = CDCMessage(consumerRecord.topic(), encodedValue)
                        val gson = new Gson()
                        QueueUtil.addFifo(PipelineEnvironment.values.cdcMesssageQueue, gson.toJson(message))
                    }
                })
                if(logTime) {
                    val totalTime=System.currentTimeMillis-before
                    logger.info("Milliseconds to add messages to FIFO queue: " + totalTime.toString)
                }
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline Kafka error: " + Throwables.getStackTraceAsString(e))
        }
    }
}
