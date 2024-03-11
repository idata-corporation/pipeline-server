package net.idata.pipeline.model

case class SparkJobConfiguration(
                                    sparkDriverMemory: String,
                                    sparkExecutorMemory: String,
                                    sparkNumExecutors: String,
                                    sparkExecutorCores: String
                                )
