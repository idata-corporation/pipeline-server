package net.idata.pipeline

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

import net.idata.pipeline.common.model.{CDCMessageThreshold, PipelineEnvironment}
import net.idata.pipeline.common.util.NotificationUtil
import net.idata.pipeline.controller.CDCConsumerRunner
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.{ApplicationArguments, ApplicationRunner}
import org.springframework.stereotype.Component

@Component
class StartupRunner extends ApplicationRunner {
    private val logger: Logger = LoggerFactory.getLogger(classOf[StartupRunner])

    @Value("${environment}")
    var environment: String = _

    @Value("${useApiKeys}")
    var useApiKeys: Boolean = _

    @Value("${cdc.debezium.kafkaTopic}")
    var cdcDebeziumKafkaTopic: String = _

    @Value("${cdc.kafka.bootstrapServer}")
    var cdcKafkaBootstrapServer: String = _

    @Value("${cdc.kafka.groupId}")
    var cdcKafkaGroupId: String = _

    @Value("${aws.region}")
    var region: String = _

    @Value("${aws.secretsManager.apiKeysSecretName}")
    var apiKeysSecretName: String = _

    @Value("${aws.secretsManager.snowflakeSecretName}")
    var snowflakeSecretName: String = _

    @Value("${aws.secretsManager.redshiftSecretName}")
    var redshiftSecretName: String = _

    @Value("${aws.sns.sendDatasetNotifications}")
    var snsSendDatasetNotifications: Boolean = _

    @Value("${aws.sns.sendCDCNotifications}")
    var snsSendCDCNotifications: Boolean = _

    @Value("${aws.sqs.sendCDCMessageQueue}")
    var snsSendCDCMessageQueue: Boolean = _

    @Value("${aws.sqs.ttlFileNotifierQueueMessages}")
    var ttlFileNotifierQueueMessages: Int = _

    @Value("${cdc.messageThreshold.objectStore}")
    var cdcThresholdObjectStore: Int = _

    @Value("${cdc.messageThreshold.redshift}")
    var cdcThresholdRedshift: Int = _

    @Value("${cdc.messageThreshold.snowflake}")
    var cdcThresholdSnowflake: Int = _

    @Override
    def run(args: ApplicationArguments): Unit =  {
        initPipelineEnvironment()
        if(PipelineEnvironment.values.cdcDebeziumKafkaTopic != null)
            initKafkaConsumer()
    }

    private def initPipelineEnvironment(): Unit = {
        // Set default values based upon the environment name
        val fileNotifierQueue = environment + "-file-notifier"
        val datasetTableName = environment + "-dataset"
        val archivedMetadataTableName = environment + "-archived-metadata"
        val datasetStatusTableName = environment + "-dataset-status"
        val fileNotifierMessageTableName = environment + "-file-notifier-message"
        val datasetPullTableName = environment + "-data-pull"

        // Send SNS dataset notifications?
        val datasetTopicArn = {
            if(snsSendDatasetNotifications)
                NotificationUtil.getTopicArn(environment + "-dataset-notification")
            else
                null
        }

        val cdcTopicArn = {
            if(snsSendCDCNotifications)
                NotificationUtil.getTopicArn(environment + "-cdc-notification.fifo")
            else
                null
        }

        val cdcMessageQueue = {
            if(snsSendCDCMessageQueue)
                environment + "-cdc-message.fifo"
            else
                null
        }

        val cdcMessageThreshold = CDCMessageThreshold(
            cdcThresholdObjectStore,
            cdcThresholdRedshift,
            cdcThresholdSnowflake)

        val pipelineEnvironment = PipelineEnvironment(
            environment,
            region,
            fileNotifierQueue,
            ttlFileNotifierQueueMessages,
            cdcMessageQueue,
            datasetTopicArn,
            cdcTopicArn,
            datasetTableName,
            archivedMetadataTableName,
            datasetStatusTableName,
            fileNotifierMessageTableName,
            datasetPullTableName,
            useApiKeys,
            apiKeysSecretName,
            snowflakeSecretName,
            redshiftSecretName,
            cdcDebeziumKafkaTopic,
            cdcKafkaBootstrapServer,
            cdcKafkaGroupId,
            cdcMessageThreshold
        )

        PipelineEnvironment.init(pipelineEnvironment)
    }

    private def initKafkaConsumer(): Unit = {
        val thread = new Thread(new CDCConsumerRunner())
        thread.start()
    }
}
