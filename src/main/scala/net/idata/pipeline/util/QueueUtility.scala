package net.idata.pipeline.util

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

import com.amazonaws.services.sqs.model.{Message, SendMessageResult}

trait QueueUtility {
    def add(queueName: String, json: String): SendMessageResult

    def addFifo(queueName: String, json: String): SendMessageResult

    def receiveMessages(queueName: String, maxMessages: Int = 1, longPolling: Boolean = false): java.util.List[Message]

    def deleteMessage(queueName: String, receiptHandle: String): Unit

    def getQueueArn(queueName: String): String
}
