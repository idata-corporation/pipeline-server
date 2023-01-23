package net.idata.pipeline

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
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

                    if(! hasMessageBeenProcessed(message.getMessageId, sqsMessage)) {
                        QueueUtil.deleteMessage(PipelineEnvironment.values.fileNotifierQueue, message.getReceiptHandle)
                        if(sqsMessage.Records != null) {
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
        val message = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.sqsMessageTableName, "id", messageID, "value")
        if(message.isEmpty) {
            val gson = new Gson
            NoSQLDbUtil.putItemJSON(PipelineEnvironment.values.sqsMessageTableName, "id", messageID, "value", gson.toJson(sqsMessageS3))
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
                logger.info("Running job: " + jobContext.pipelineToken + ", " + jobContext.config.name + ", " + jobContext.state.toString)
        })
    }

    private def isDatabaseJobForDatasetAlreadyRunning(jobContext: JobContext): Boolean = {
        if(jobContext.config.destination.database != null) {
            // Find the jobs with the same database table name
            val jobContextsWithDbTableName = GlobalJobContext.getAll.filter(_.config.destination.database.table.compareTo(jobContext.config.destination.database.table) == 0).toList

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
                logger.info("Job completed: " + jobContext.pipelineToken + ", " + jobContext.config.name + ", COMPLETED")
                GlobalJobContext.replaceJobContext(jobContext = jobContext.copy(state = COMPLETED))
            }
        })
    }

    private def isAppInitialized: Boolean = {
        PipelineEnvironment != null && PipelineEnvironment.values != null
    }
}

