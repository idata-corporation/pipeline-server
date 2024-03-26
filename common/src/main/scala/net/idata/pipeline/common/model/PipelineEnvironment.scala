package net.idata.pipeline.common.model

import net.idata.pipeline.common.model.cdc.CDCMessageThreshold
import net.idata.pipeline.common.model.spark.SparkProperties

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

object PipelineEnvironment {
    var values: PipelineEnvironment = _

    def init(environment: PipelineEnvironment): Unit = {
        values = environment
    }
}

case class PipelineEnvironment(
                                  environment: String,
                                  region: String,
                                  fileNotifierQueue: String,
                                  ttlFileNotifierQueueMessages: Int,
                                  cdcMesssageQueue: String,
                                  datasetTopicArn: String,
                                  cdcTopicArn: String,
                                  datasetTableName: String,
                                  archivedMetadataTableName: String,
                                  datasetStatusTableName: String,
                                  fileNotifierMessageTableName: String,
                                  dataPullTableName: String,
                                  cdcMapperTableName: String,
                                  useApiKeys: Boolean,
                                  apiKeysSecretName: String,
                                  snowflakeSecretName: String,
                                  redshiftSecretName: String,
                                  cdcDebeziumKafkaTopic: String,
                                  kafkaBootstrapServer: String,
                                  kafkaGroupId: String,
                                  cdcMessageThreshold: CDCMessageThreshold,
                                  sparkProperties: SparkProperties
                              )
