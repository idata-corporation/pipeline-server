package net.idata.pipeline.common.model

case class DebeziumConfig(
                             kafkaBootstrapServer: String,
                             kafkaGroupId: String,
                             kafkaTopic: String,
                             kafkaTopicPollingInterval: Int
                         )
