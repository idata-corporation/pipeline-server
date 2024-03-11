package net.idata.pipeline.common.model

case class EmrProperties(
                            masterNodeIp: String
                        )

case class EksEmrProperties(
                               virtualClusterId: String,
                               configurationFileUrl: String,
                               releaseLabel: String,
                               executionRoleArn: String,
                               monitoringLogUri: String,
                               cloudWatchLogGroupName: String
                           )
