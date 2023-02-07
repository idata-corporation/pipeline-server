package net.idata.pipeline

/*
IData Pipeline
Copyright (C) 2023 IData Corporation (http://www.idata.net)

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

Author(s): Todd Fearn
*/


import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.controller.{FileNotifier, JobRunner}
import net.idata.pipeline.model._
import net.idata.pipeline.model.aws.SQSMessageS3
import net.idata.pipeline.util.{NoSQLDbUtil, QueueUtil}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

import java.util.Calendar
import scala.collection.JavaConverters._

@Component
class ScheduledBatchTasks {
    private val logger: Logger = LoggerFactory.getLogger(classOf[ScheduledBatchTasks])

    @Scheduled(fixedRateString = "${schedule.checkFileNotifierQueue}")
    private def checkFileNotifierQueue(): Unit = {
        try {
            if(isAppInitialized) {
                val messages = QueueUtil.receiveMessages(PipelineEnvironment.values.fileNotifierQueue, maxMessages = 10, longPolling = true)

                val gson = new Gson
                messages.asScala.foreach(message => {
                    val sqsMessage = gson.fromJson(message.getBody, classOf[SQSMessageS3])
                    QueueUtil.deleteMessage(PipelineEnvironment.values.fileNotifierQueue, message.getReceiptHandle)

                    if(sqsMessage != null && sqsMessage.Records != null) {
                        if(! hasMessageBeenProcessed(message.getMessageId, sqsMessage)) {
                            sqsMessage.Records.asScala.map(record => {
                                (record.s3.bucket.name, record.s3.`object`.key)
                            }).toMap
                                .foreach(record => {
                                    newFileReceived(record._1, record._2)
                                })
                        }
                    }
                })
            }
        } catch {
            case e: Exception =>
                logger.error("checkFileNotifierQueue error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def hasMessageBeenProcessed(messageID: String, sqsMessageS3: SQSMessageS3): Boolean = {
        // Check the NoSQL table to determine if this message has already been processed
        val message = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.fileNotifierMessageTableName, "id", messageID, "value")
        if(message.isEmpty) {
            // Create a future TTL
            val now = Calendar.getInstance
            now.add(Calendar.DATE, PipelineEnvironment.values.ttlFileNotifierQueueMessages) // Days in future for TTL to delete this new entry from the table
            val epoch = now.getTime.getTime
            logger.info("File notifier queue message TTL: " + epoch.toString)

            // Write out the SQS Message ID with the future TTL
            NoSQLDbUtil.setItemNameValue(PipelineEnvironment.values.fileNotifierMessageTableName, "id", messageID, "ttl", epoch.toString)
            false
        }
        else
            true
    }

    private def newFileReceived(bucket: String, key: String): Unit = {
        val jobContext = new FileNotifier().process(bucket, key)
        GlobalJobContext.addJobContext(jobContext)
    }

    @Scheduled(fixedRateString = "${schedule.findJobsToStart}")
    private def findJobsToStart(): Unit = {
        try {
            if(isAppInitialized) {
                startJobs()
                checkExistingJobs()
            }
        }
        catch {
            case e: Exception =>
                logger.error("findJobsToStart error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def startJobs(): Unit = {
        GlobalJobContext.getAll.foreach(jobContext => {
            if(jobContext.state == INITIALIZED) {
                if(!isDatabaseJobForDatasetAlreadyRunning(jobContext))
                    startJob(jobContext)
            }
        })

        // Show running jobs
        GlobalJobContext.getAll.foreach(jobContext => {
            if(jobContext.state ==  PROCESSING)
                logger.info(jobContext.pipelineToken + ": dataset: " + jobContext.config.name + ", " + jobContext.state.toString)
        })
    }

    private def isDatabaseJobForDatasetAlreadyRunning(jobContext: JobContext): Boolean = {
        if(jobContext.config.destination.database != null) {
            // Find the jobs with the same database table name
            val jobContextsWithDbTableName = GlobalJobContext.getAll.flatMap(jc => {
                if(jc.config.destination.database != null && jc.config.destination.database.table.compareTo(jobContext.config.destination.database.table) == 0)
                    Some(jc)
                else
                    None
            }).toList

            // Do any exist that are running?
            jobContextsWithDbTableName.exists(_.state == PROCESSING)
        }
        else
            false
    }

    private def startJob(jobContext: JobContext): Unit = {
        logger.info("Starting job for the dataset: " + jobContext.config.name)

        // Start the db loading process
        val thread = new Thread(new JobRunner(jobContext))
        thread.start()
        GlobalJobContext.replaceJobContext(jobContext = jobContext.copy(state = PROCESSING, thread = thread))
    }

    private def checkExistingJobs(): Unit ={
        GlobalJobContext.getAll.foreach(jobContext => {
            if(jobContext.state == PROCESSING && jobContext.thread != null && !jobContext.thread.isAlive) {
                logger.info(jobContext.pipelineToken + ": dataset: " + jobContext.config.name + ", COMPLETED")
                GlobalJobContext.replaceJobContext(jobContext = jobContext.copy(state = COMPLETED))
            }
        })
    }

    private def isAppInitialized: Boolean = {
        PipelineEnvironment != null && PipelineEnvironment.values != null
    }
}

