package net.idata.pipeline.common.model

case class SparkProperties(
                              useEmr: Boolean,
                              useEksEmr: Boolean,
                              emrProperties: EmrProperties,
                              eksEmrProperties: EksEmrProperties
                          )
