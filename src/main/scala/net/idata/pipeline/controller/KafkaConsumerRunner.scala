package net.idata.pipeline.controller

import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.common.base.Throwables
import net.idata.pipeline.model.{DatasetConfig, DebeziumMessage, PipelineEnvironment, PipelineException}
import net.idata.pipeline.util.{DatasetConfigIO, ObjectStoreUtil}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayInputStream
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Date, Properties}
import scala.collection.JavaConverters._

class KafkaConsumerRunner() extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaConsumerRunner])

    def run(): Unit = {
        try {
            val properties = new Properties()
            properties.put("bootstrap.servers", "b-1.kafkadebezium.7zbc63.c25.kafka.us-east-1.amazonaws.com:9092")
            properties.put("group.id", "idata-group")
            properties.put("key.deserializer", classOf[StringDeserializer])
            properties.put("value.deserializer", classOf[StringDeserializer])

            val consumer: Consumer[String, String] = new KafkaConsumer[String, String](properties)
            val pattern = Pattern.compile(PipelineEnvironment.values.cdcDebeziumKafkaTopic + ".*")
            consumer.subscribe(pattern)
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(100)
                records.asScala.foreach(consumerRecord => {
                    logger.info("Message received, topic: " + consumerRecord.topic() + ", key: " + consumerRecord.key() + ", " + consumerRecord.value())
                    val debeziumMessage = parseMessage(PipelineEnvironment.values.cdcDebeziumKafkaTopic, consumerRecord.topic(), consumerRecord.value())
                    logger.info("debezium message: " + debeziumMessage.toString)

                    val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, debeziumMessage.datasetName)
                    if(config == null) {
                        logger.error("Pipeline Kafka message error, Dataset configuration: " + debeziumMessage.datasetName + " was not found in the NoSQL table: " + PipelineEnvironment.values.datasetTableName)
                        // TODO - put this message in a queue or something to save it
                    }
                    else
                        processMessage(config, debeziumMessage)
                })
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline Kafka error: " + Throwables.getStackTraceAsString(e))
        }
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

    private def processMessage(config: DatasetConfig, message: DebeziumMessage): Unit = {
        val data = {
            val header = message.after.keys.mkString(config.source.fileAttributes.csvAttributes.delimiter)
            val row = message.after.values.mkString(config.source.fileAttributes.csvAttributes.delimiter)
            List(header, row).mkString("\n")
        }

        val rawFilename = {
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH-mm-ss-SSS")
            val date = dateFormat.format(new Date())
            message.datasetName + "." + date + "." + System.currentTimeMillis().toString + ".dataset.csv"
        }

        // Write the data to the raw bucket
        val byteArray = data.getBytes
        val contentLength = byteArray.length
        val metadata = new ObjectMetadata()
        metadata.setContentLength(contentLength)
        val fileStream = new ByteArrayInputStream(byteArray)
        val path = "s3://" + PipelineEnvironment.values.environment + "-raw/temp/" + message.datasetName + "/" + rawFilename
        ObjectStoreUtil.writeBucketObjectFromStream(ObjectStoreUtil.getBucket(path), ObjectStoreUtil.getKey(path), fileStream, metadata)
    }
}
