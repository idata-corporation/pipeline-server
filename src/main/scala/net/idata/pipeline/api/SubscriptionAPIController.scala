package net.idata.pipeline.api

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
import net.idata.pipeline.model.{PipelineEnvironment, PipelineException, Subscription}
import net.idata.pipeline.util.{APIKeyValidator, NotificationUtil}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation._

@RestController
@CrossOrigin(origins = Array("*"), methods = Array(RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS))
class SubscriptionAPIController {
    private val logger: Logger = LoggerFactory.getLogger(classOf[SubscriptionAPIController])

    @GetMapping(path = Array("/subscriptions"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getSubscriptions(@RequestHeader(name = "x-api-key", required = false) apiKey: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /subscriptions called")
            APIKeyValidator.validate(apiKey)

            val subscriptions = NotificationUtil.getSubscribers(PipelineEnvironment.values.notifyTopicArn)
            val gson = new Gson()
            val json = gson.toJson(subscriptions)

            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @PostMapping(path = Array("/subscription"), consumes = Array(MediaType.APPLICATION_JSON_VALUE), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def subscribe(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                   @RequestBody subscription: Subscription): ResponseEntity[String] = {
        try {
            logger.info("API endpoint POST /subscription called with protocol: " + subscription.protocol + ", endpoint: " + subscription.endpointArn)
            APIKeyValidator.validate(apiKey)

            if(subscription.endpointArn == null)
                throw new PipelineException("'endpointArn' is a required value")
            if(subscription.protocol == null)
                throw new PipelineException("'protocol' is a required value")

            val subscriptionWithTopicArn = subscription.copy(topicArn = PipelineEnvironment.values.notifyTopicArn)
            val s = NotificationUtil.addSubscription(subscriptionWithTopicArn)
            val gson = new Gson
            val json = gson.toJson(s)

            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @GetMapping(path = Array("/subscription"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getSubscription(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                        @RequestParam(name = "subscriptionarn") subscriptionArn: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /subscription called for subscriptionarn: " + subscriptionArn)
            APIKeyValidator.validate(apiKey)

            val subscription = NotificationUtil.getSubscription(subscriptionArn)
            val gson = new Gson
            val json = gson.toJson(subscription)

            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @DeleteMapping(path = Array("/subscription"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def unsubscribe(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                    @RequestParam(name = "subscriptionarn") subscriptionArn: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint DELETE /subscription called for subscriptionarn: " + subscriptionArn)
            APIKeyValidator.validate(apiKey)

            NotificationUtil.deleteSubscription(subscriptionArn)

            new ResponseEntity[String](HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }
}
