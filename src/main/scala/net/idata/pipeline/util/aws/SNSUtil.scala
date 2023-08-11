package net.idata.pipeline.util.aws

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

import com.amazonaws.services.sns.model.{MessageAttributeValue, PublishRequest, PublishResult, SubscribeRequest}
import com.amazonaws.services.sns.{AmazonSNS, AmazonSNSClientBuilder}
import net.idata.pipeline.model.{PipelineException, Subscription}
import net.idata.pipeline.util.NotificationUtility

import scala.collection.JavaConverters._
import scala.collection.mutable

class SNSUtil(val sns: AmazonSNS) extends NotificationUtility {
    override def add(topicArn: String, json: String): PublishResult = {
        val publishRequest = new PublishRequest().withTopicArn(topicArn).withMessage(json)
        sns.publish(publishRequest)
    }

    override def add(topicArn: String, json: String, filter: Map[String, String]): PublishResult = {
        val messageAttributeMap = filter.map { case (name, value) =>
            val messageAttribute = {
                if(value.contains(","))
                    new MessageAttributeValue().withDataType("String.Array").withStringValue(value)
                else
                    new MessageAttributeValue().withDataType("String").withStringValue(value)
            }
            (name, messageAttribute)
        }.asJava

        val publishRequest = new PublishRequest()
            .withTopicArn(topicArn)
            .withMessage(json)
            .withMessageAttributes(messageAttributeMap)

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