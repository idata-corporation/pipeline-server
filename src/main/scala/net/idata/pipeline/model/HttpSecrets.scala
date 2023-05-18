package net.idata.pipeline.model

case class HttpSecrets(
                          enabled: Boolean,
                          apiKeysUrl: String,
                          snowflakeUrl: String,
                          redshiftUrl: String
                      )
