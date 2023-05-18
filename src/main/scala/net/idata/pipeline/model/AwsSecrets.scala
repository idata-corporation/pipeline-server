package net.idata.pipeline.model

case class AwsSecrets(
                         enabled: Boolean,
                         apiKeysName: String,
                         snowflakeName: String,
                         redshiftName: String
                     )
