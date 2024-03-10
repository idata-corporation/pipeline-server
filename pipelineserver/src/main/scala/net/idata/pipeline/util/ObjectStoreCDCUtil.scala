package net.idata.pipeline.util

import net.idata.pipeline.common.model.{DatasetConfig, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, ObjectStoreSQLUtil}
import net.idata.pipeline.model.DebeziumMessage
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant

class ObjectStoreCDCUtil {
    private val logger: Logger = LoggerFactory.getLogger(classOf[ObjectStoreCDCUtil])
    private val messageThreshold = 10

    def process(groupOfMessages: List[(String, DatasetConfig, DebeziumMessage)]): Unit = {
        val config = groupOfMessages.head._2
        val containsDeletes = groupOfMessages.find(_._3.isDelete == true)

        if(containsDeletes.isDefined && !config.destination.objectStore.useIceberg)
            throw new PipelineException("CDC 'deletes' cannot be written to non Apache Iceberg formatted parquet files")

        // If the number of messages is less than the threshold or there are deletes, use JDBC to process
        if (groupOfMessages.length < messageThreshold || containsDeletes.isDefined) {
            val sqlList = groupOfMessages.map { case (_, config, message) =>
                if (message.isInsert)
                    CDCUtil.insertCreateSQL(config, message)
                else if (message.isUpdate)
                    CDCUtil.updateCreateSQL(config, message)
                else
                    CDCUtil.deleteCreateSQL(config, message)
            }
            sqlList.foreach(sql => {
                logger.info("CDC SQL for Athena: " + sql)
                val config = groupOfMessages.head._2
                val glueDbName = config.source.schemaProperties.dbName
                val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
                ObjectStoreSQLUtil.sql(glueDbName, sql, outputPath)
            })
        }
        else {
            // If the number of messages is greater and there are no deletes, create a file and drop it into the Pipeline
            val messages = groupOfMessages.map(_._3)
            CDCUtil.createFile(config, messages)
        }
    }
}