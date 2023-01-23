package net.idata.pipeline.util.aws

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

import com.amazonaws.services.sqs.model._
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import net.idata.pipeline.util.{GuidV5, QueueUtility}

class SQSUtility(val sqs: AmazonSQS) extends QueueUtility {
  override def add(queueName: String, json: String): SendMessageResult = {
    add(getQueueUrl(queueName), null, json)
  }

  override def addFifo(queueName: String, json: String): SendMessageResult = {
    val messageGroupId: String = "pipeline-message-group"  // The pipeline does not require individual message groups
    add(getQueueUrl(queueName), messageGroupId, json)
  }

  override def receiveMessages(queueName: String, maxMessages: Int = 1, longPolling: Boolean = false): java.util.List[Message] = {
    val queueUrl = sqs.getQueueUrl(queueName).getQueueUrl
    val receiveMessageRequest = {
        if(longPolling)
            new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(maxMessages)
                .withVisibilityTimeout(3)
                .withWaitTimeSeconds(3)
        else
            new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(maxMessages)
    }
    sqs.receiveMessage(receiveMessageRequest).getMessages
  }

  override def deleteMessage(queueName: String, receiptHandle: String): Unit = {
    val queueUrl = sqs.getQueueUrl(queueName).getQueueUrl
    sqs.deleteMessage(queueUrl, receiptHandle)
  }

  override def getQueueArn(queueName: String): String = {
    val queueUrl = getQueueUrl(queueName)
    val getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl).withAttributeNames("QueueArn")
    val attributes = sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes
    attributes.get("QueueArn")
  }

  private def getQueueUrl(queueName: String): String = {
    sqs.getQueueUrl(queueName).getQueueUrl
  }

  private def add(queueUrl: String, messageGroupID: String, json: String): SendMessageResult = {
    val message =
      if (messageGroupID != null) {
        // If messageGroupID is not null, the queue sent to must be a FIFO queue
        val messageDedupilicationId = GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString
        new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody(json)
            .withMessageGroupId(messageGroupID)
            .withMessageDeduplicationId(messageDedupilicationId)
      }
      else {
        new SendMessageRequest().
            withQueueUrl(queueUrl).
            withMessageBody(json)
      }

      sqs.sendMessage(message)
  }
}

object SQSUtilBuilder {
  private lazy val sqs = AmazonSQSClientBuilder.defaultClient

  def build(): QueueUtility = new SQSUtility(sqs)
}
