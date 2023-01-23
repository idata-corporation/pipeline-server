package net.idata.pipeline.util

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
*/

import com.amazonaws.services.sqs.model.{Message, SendMessageResult}

trait QueueUtility {
    def add(queueName: String, json: String): SendMessageResult

    def addFifo(queueName: String, json: String): SendMessageResult

    def receiveMessages(queueName: String, maxMessages: Int = 1, longPolling: Boolean = false): java.util.List[Message]

    def deleteMessage(queueName: String, receiptHandle: String): Unit

    def getQueueArn(queueName: String): String
}
