package net.idata.pipeline.util

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

import com.google.gson.{Gson, GsonBuilder}
import net.idata.pipeline.common.model.PipelineEnvironment
import net.idata.pipeline.common.util.NotificationUtil
import net.idata.pipeline.model.CDCMessage
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CDCMessagePublisher(messages: List[CDCMessage]) extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[CDCMessagePublisher])

    def run(): Unit = {
        logger.info("Number of messages received to publish to SNS: " + messages.size.toString)

        // Break the total size of messages to < 256Kb (SNS max) and publish each block
        val messageList = new ListBuffer[CDCMessage]()
        var messageListSize = 0

        messages.foreach(cdcMessage => {
            // Determine the size of the message
            val gson = new Gson()
            val size = gson.toJson(cdcMessage).length

            // The message list cannot exceed 255Kb, SNS limit is 256Kb
            if (size + messageListSize >= (255 * 1024)) {
                publish(messageList.toList)
                messageList.clear()
                messageList += cdcMessage
            }
            else
                messageList += cdcMessage
            messageListSize = messageListSize + size
        })
        if(messageList.nonEmpty)
            publish(messageList.toList)
    }

    private def publish(messages: List[CDCMessage]): Unit = {
        val gson = new GsonBuilder().disableHtmlEscaping().create()

        // The messages are for the same database, schema, table.  Retrieve the attributes from the first message
        val firstMessage = messages.head
        val attributes = new java.util.HashMap[String, String]
        attributes.put("database", firstMessage.databaseName)
        attributes.put("schema", firstMessage.schemaName)
        attributes.put("table", firstMessage.tableName)

        // Send notification
        logger.info("Sending message for database: " + firstMessage.databaseName + ", schema: " + firstMessage.schemaName + ", table: " + firstMessage.tableName + " to topic: " + PipelineEnvironment.values.cdcConfig.publishSNSTopicArn)
        val cdcMessages = gson.toJson(messages.asJava)
        NotificationUtil.addFifo(PipelineEnvironment.values.cdcConfig.publishSNSTopicArn, cdcMessages, attributes.asScala.toMap)
    }
}