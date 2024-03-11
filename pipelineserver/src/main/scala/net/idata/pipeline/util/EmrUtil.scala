package net.idata.pipeline.util

import net.idata.pipeline.common.model.PipelineEnvironment
import net.idata.pipeline.common.util.NoSQLDbUtil
import org.slf4j.{Logger, LoggerFactory}

object EmrUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val configKeyName = "name"
    private val configKeyValue = "emrMasterNodeIp"
    private val configValueName = "value"

    def isEmrRunning: Boolean = {
        val clusterId = getVirtualClusterId(PipelineEnvironment.values.configTableName)

        if(clusterId == null) {
            logger.warn("The 'emrVirtualClusterId' does not exist in the DynamoDb table: " + PipelineEnvironment.values.configTableName)
            false
        }
        else {
            val state = ElasticMapReduceUtil.getClusterState(clusterId)
            state match {
                case "RUNNING" | "WAITING" =>
                    true
                case "STARTING" | "BOOTSTRAPPING" =>
                    logger.info("Waiting for the EMR cluster to start, cluster ID: " + clusterId)
                    false
                case _ =>
                    logger.warn("The 'emrVirtualClusterId': " + clusterId + " in the DynamoDb table: " + PipelineEnvironment.values.configTableName + " has a state of " + state +
                        " Create a new cluster and store the clusterID in the DynamoDb table.")
                    false
            }
        }
    }

    private def getVirtualClusterId(configTableName: String): String = {
        // Get the cluster ID from DynamoDb or start a new cluster
        val json = NoSQLDbUtil.getItemJSON(configTableName, configKeyName, configKeyValue, configValueName).orNull
        if(json != null)
            json.replace("\"", "")
        else
            null
    }

    def getEmrMasterNodeIp(configTableName: String): String = {
        // Get the cluster ID from DynamoDb or start a new cluster
        val json = NoSQLDbUtil.getItemJSON(configTableName, configKeyName, configKeyValue, configValueName).orNull
        if(json != null)
            json.replace("\"", "")
        else
            null
    }

/*
    private def createCluster(): Unit = {
        if(PipelineEnvironment.values.sparkProperties.emrProperties.autoCreate) {
            val clusterId = ElasticMapReduceUtil.createCluster(PipelineEnvironment.values.sparkProperties.emrProperties)
            logger.info("The YAML property EMR autoCreate is set to true, creating a new EMR cluster with virtual cluster ID: " + clusterId)

            PipelineEnvironment.putEmrVirtualClusterId(clusterId)

            // Save the cluster ID in the DynamoDb config table
            val gson = new Gson
            NoSQLDbUtil.putItemJSON(PipelineEnvironment.values.configTableName, configKeyName, configKeyName, configValueName, gson.toJson(clusterId))
        }
    }
*/
}