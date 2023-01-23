package net.idata.pipeline

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

import net.idata.pipeline.model.PipelineEnvironment
import net.idata.pipeline.util.NotificationUtil
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.{ApplicationArguments, ApplicationRunner}
import org.springframework.stereotype.Component

@Component
class StartupRunner extends ApplicationRunner {
    @Value("${environment}")
    var environment: String = _

    @Value("${aws.region}")
    var region: String = _

    @Value("${aws.secretsManager.apiKeysSecretName}")
    var apiKeysSecretName: String = _

    @Value("${aws.secretsManager.snowflakeSecretName}")
    var snowflakeSecretName: String = _

    @Value("${aws.secretsManager.redshiftSecretName}")
    var redshiftSecretName: String = _

    @Value("${aws.sns.sendNotifications}")
    var snsSendNotifications: Boolean = _

    @Override
    def run(args: ApplicationArguments): Unit =  {
        initPipelineEnvironment()
    }

    private def initPipelineEnvironment(): Unit = {
        // Set default values based upon the environment name
        val fileNotifierQueue = environment + "-file-notifier"
        val datasetTableName = environment + "-dataset"
        val archivedMetadataTableName = environment + "-archived-metadata"
        val datasetStatusTableName = environment + "-dataset-status"
        val sqsMessageTableName = environment + "-sqs-message"

        // Send SNS notifications?
        val notifyTopicArn = {
            if(snsSendNotifications)
                NotificationUtil.getTopicArn(environment + "-dataset-notification")
            else
                null
        }

        val snowflakeEnvironment = PipelineEnvironment(
            environment,
            region,
            fileNotifierQueue,
            notifyTopicArn,
            datasetTableName,
            archivedMetadataTableName,
            datasetStatusTableName,
            sqsMessageTableName,
            apiKeysSecretName,
            snowflakeSecretName,
            redshiftSecretName
        )

        PipelineEnvironment.init(snowflakeEnvironment)
    }
}
