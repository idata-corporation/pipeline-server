package net.idata.pipeline

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