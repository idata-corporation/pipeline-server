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

import com.amazonaws.services.sns.model.{PublishRequest, PublishResult, SubscribeRequest}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import net.idata.pipeline.model.{PipelineException, Subscription}
import net.idata.pipeline.util.NotificationUtility

import scala.collection.JavaConverters._
import scala.collection.mutable

class SNSUtil(val sns: AmazonSNS) extends NotificationUtility {
    override def add(topicArn: String, json: String): PublishResult = {
        add(topicArn, null, json)
    }

    override def addFifo(topicArn: String, json: String): PublishResult = {
        val messageGroupId: String = "snowflakeloader-message-group" // The snowflakeloader does not require individual message groups
        add(topicArn, messageGroupId, json)
    }

    private def add(topicArn: String, messageGroupID: String, json: String): PublishResult = {
        val publishRequest = {
            // TODO: Add this back when Databricks upgrades their AWS SDK version
            /*
            if (messageGroupID != null) {
                val messageDedupilicationId = GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString

                // If messageGroupID is not null, the topic sent to must be a FIFO topic
                new PublishRequest()
                    .withTopicArn(topicArn)
                    .withMessageGroupId(messageGroupID)
                    .withMessageDeduplicationId(messageDedupilicationId)
                    .withMessage(json)
            }
            else {
*/
            new PublishRequest()
                .withTopicArn(topicArn)
                .withMessage(json)
            //}
        }

        sns.publish(publishRequest)
    }

    def addSubscription(subscription: Subscription): Subscription = {
        val subscribeRequest = {
            val attributes = mutable.Map[String, String]()
            attributes += ("RawMessageDelivery" -> "true")
            if(subscription.filterPolicy != null)
                attributes += ("FilterPolicy" -> subscription.filterPolicy)
            new SubscribeRequest(subscription.topicArn, subscription.protocol, subscription.endpointArn).withAttributes(attributes.asJava)
        }
        val subscriptionArn = sns.subscribe(subscribeRequest).getSubscriptionArn

        val attributes = sns.getSubscriptionAttributes(subscriptionArn).getAttributes
        Subscription(
            attributes.get("Owner"),
            subscriptionArn,
            subscription.topicArn,
            subscription.endpointArn,
            subscription.protocol,
            subscription.filterPolicy
        )
    }

    def getSubscription(subscriptionArn: String): Subscription = {
        val attributes = sns.getSubscriptionAttributes(subscriptionArn).getAttributes
        Subscription(
            attributes.get("Owner"),
            subscriptionArn,
            attributes.get("TopicArn"),
            attributes.get("Endpoint"),
            attributes.get("Protocol"),
            attributes.get("FilterPolicy")
        )
    }

    def deleteSubscription(subscriptionArn: String): Unit = {
        sns.unsubscribe(subscriptionArn)
    }

    def getSubscribers(topicArn: String): java.util.List[Subscription] = {
        val subscriptions = sns.listSubscriptionsByTopic(topicArn).getSubscriptions
        subscriptions.asScala.map(s => {
            val attributes = sns.getSubscriptionAttributes(s.getSubscriptionArn).getAttributes
            val filterPolicy = attributes.get("FilterPolicy")

            Subscription(
                s.getOwner,
                s.getSubscriptionArn,
                s.getTopicArn,
                s.getEndpoint,
                s.getProtocol,
                filterPolicy
            )
        }).toList.asJava
    }

    def getTopicArn(topicName: String): String = {
        val listTopicsResult = sns.listTopics()
        val arns = listTopicsResult.getTopics.asScala.flatMap(topic => {
            if (topic.getTopicArn.endsWith(topicName))
                Some(topic.getTopicArn)
            else
                None
        }).toList
        if(arns.size != 1)
            throw new PipelineException("Could not find the ARN for SNS topic name: " + topicName)
        arns.head
    }
}

object SNSUtilBuilder {
    private lazy val sns = AmazonSNSClientBuilder.defaultClient

    def build(): NotificationUtility = new SNSUtil(sns)
}