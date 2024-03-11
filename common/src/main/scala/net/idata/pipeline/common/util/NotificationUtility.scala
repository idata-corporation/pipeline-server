package net.idata.pipeline.common.util

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

import com.amazonaws.services.sns.model.PublishResult
import net.idata.pipeline.common.model.Subscription

trait NotificationUtility {
    def add(topicArn: String, json: String): PublishResult
    def add(topicArn: String, json: String, filter: Map[String, String]): PublishResult
    def addFifo(topicArn: String, json: String, filter: Map[String, String]): PublishResult
    def addSubscription(subscription: Subscription): Subscription
    def getSubscription(subscriptionArn: String): Subscription
    def deleteSubscription(subscriptionArn: String): Unit
    def getSubscribers(topicArn: String): java.util.List[Subscription]
    def getTopicArn(topicName: String): String
}