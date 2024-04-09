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

import net.idata.pipeline.common.model.{CDCConfig, CDCWriteMessageThreshold, DebeziumConfig, IDataCDCConfig, PipelineEnvironment}
import net.idata.pipeline.common.util.NotificationUtil
import net.idata.pipeline.controller.{DebeziumCDCRunner, IDataCDCRunner}
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

    @Value("${aws.region}")
    var region: String = _

    @Value("${aws.secretsManager.apiKeysSecretName}")
    var apiKeysSecretName: String = _

    @Value("${aws.secretsManager.snowflakeSecretName}")
    var snowflakeSecretName: String = _

    @Value("${aws.secretsManager.redshiftSecretName}")
    var redshiftSecretName: String = _

    @Value("${aws.secretsManager.postgresSecretName}")
    var postgresSecretName: String = _

    @Value("${aws.sns.sendDatasetNotifications}")
    var snsSendDatasetNotifications: Boolean = _

    @Value("${aws.sqs.ttlFileNotifierQueueMessages}")
    var ttlFileNotifierQueueMessages: Int = _

    @Value("${cdc.enabled}")
    var cdcEnabled: Boolean = _

    @Value("${cdc.processMessages.enabled}")
    var cdcProcessMessagesEnabled: Boolean = _

    @Value("${cdc.processMessages.datasetMapperTableName}")
    var cdcDatasetMapperTableName: String = _

    @Value("${cdc.publishMessages.enabled}")
    var cdcPublishMessagesEnabled: Boolean = _

    @Value("${cdc.publishMessages.sns.topicArn}")
    var cdcPublishMessagesSNSTopicArn: String = _

    @Value("${cdc.writeMessageThreshold.objectStore}")
    var cdcWriteThresholdObjectStore: Int = _

    @Value("${cdc.writeMessageThreshold.redshift}")
    var cdcWriteThresholdRedshift: Int = _

    @Value("${cdc.writeMessageThreshold.snowflake}")
    var cdcWriteThresholdSnowflake: Int = _

    @Value("${cdc.debezium.enabled}")
    var debeziumEnabled: Boolean = _

    @Value("${cdc.debezium.kafka.bootstrapServer}")
    var debeziumKafkaBootstrapServer: String = _

    @Value("${cdc.debezium.kafka.groupId}")
    var debeziumKafkaGroupId: String = _

    @Value("${cdc.debezium.kafka.topic}")
    var debeziumKafkaTopic: String = _

    @Value("${cdc.debezium.kafka.topicPollingInterval}")
    var debeziumKafkaTopicPollingInterval: Int = _

    @Value("${cdc.idata.enabled}")
    var idataCDCEnabled: Boolean = _

    @Value("${cdc.idata.database.type}")
    var idataCDCDatabaseType: String = _

    @Value("${cdc.idata.database.name}")
    var idataCDCDatabaseName: String = _

    @Value("${cdc.idata.database.secretName}")
    var idataCDCDatabaseSecretName: String = _

    @Value("${cdc.idata.database.includeTables}")
    var idataCDCDatabaseIncludeTables: java.util.ArrayList[String] = _

    @Value("${cdc.idata.pollingInterval}")
    var idataCDCPollingInterval: Int = _

    @Override
    def run(args: ApplicationArguments): Unit =  {
        initPipelineEnvironment()
        if(cdcEnabled && debeziumEnabled)
            initDebeziumCDCRunner()
        if(cdcEnabled && idataCDCEnabled)
            initIDataCDCRunner()
    }

    private def initPipelineEnvironment(): Unit = {
        // Set default values based upon the environment name
        val fileNotifierQueue = environment + "-file-notifier"
        val datasetTableName = environment + "-dataset"
        val archivedMetadataTableName = environment + "-archived-metadata"
        val datasetStatusTableName = environment + "-dataset-status"
        val fileNotifierMessageTableName = environment + "-file-notifier-message"
        val datasetPullTableName = environment + "-data-pull"
        val cdcLastReadTableName = environment + "-cdc-last-read"

        // Send SNS dataset notifications?
        val datasetTopicArn = {
            if(snsSendDatasetNotifications)
                NotificationUtil.getTopicArn(environment + "-dataset-notification")
            else
                null
        }

        // CDC
        //
        val cdcWriteMessageThreshold = {
            if(cdcEnabled) {
                CDCWriteMessageThreshold(
                    cdcWriteThresholdObjectStore,
                    cdcWriteThresholdRedshift,
                    cdcWriteThresholdSnowflake)
            }
            else
                null
        }

        val debeziumConfig = {
            if(cdcEnabled && debeziumEnabled) {
                DebeziumConfig(
                    debeziumKafkaBootstrapServer,
                    debeziumKafkaGroupId,
                    debeziumKafkaTopic,
                    debeziumKafkaTopicPollingInterval)
            }
            else
                null
        }

        val idataCDCConfig = {
            if(cdcEnabled && idataCDCEnabled) {
                IDataCDCConfig(
                    idataCDCEnabled,
                    idataCDCDatabaseType,
                    idataCDCDatabaseSecretName,
                    idataCDCDatabaseName,
                    idataCDCDatabaseIncludeTables,
                    idataCDCPollingInterval,
                    cdcLastReadTableName
                )
            }
            else
                null
        }

        val cdcConfig = {
            if(cdcEnabled) {
                CDCConfig(
                    cdcEnabled,
                    cdcProcessMessagesEnabled,
                    cdcPublishMessagesEnabled,
                    cdcPublishMessagesSNSTopicArn,
                    cdcWriteMessageThreshold,
                    debeziumConfig,
                    idataCDCConfig,
                    cdcDatasetMapperTableName
                )
            }
            else
                null
        }

        val pipelineEnvironment = PipelineEnvironment(
            environment,
            region,
            fileNotifierQueue,
            ttlFileNotifierQueueMessages,
            datasetTopicArn,
            datasetTableName,
            archivedMetadataTableName,
            datasetStatusTableName,
            fileNotifierMessageTableName,
            datasetPullTableName,
            useApiKeys,
            apiKeysSecretName,
            snowflakeSecretName,
            redshiftSecretName,
            postgresSecretName,
            cdcConfig
        )

        PipelineEnvironment.init(pipelineEnvironment)
    }

    private def initDebeziumCDCRunner(): Unit = {
        val thread = new Thread(new DebeziumCDCRunner())
        thread.start()
    }

    private def initIDataCDCRunner(): Unit = {
        val thread = new Thread(new IDataCDCRunner())
        thread.start()
    }
}
