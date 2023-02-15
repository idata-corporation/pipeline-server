package net.idata.pipeline.model

case class RedshiftSecrets(
                              username: String,
                              password: String,
                              jdbcUrl: String,
                              dbRole: String
                          )
