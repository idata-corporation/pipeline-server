package net.idata.pipeline.util.spark

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

import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder
import com.amazonaws.services.emrcontainers.model._
import com.google.gson.Gson
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.ObjectStoreUtil
import net.idata.pipeline.model.spark.SparkJobStatus
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object EksEmrUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    // Base64 encoded arguments are required because of a bug in YARN with 2 brackets }} - https://issues.apache.org/jira/browse/SPARK-17814
    def executeSparkJob(file: String, className: String, pyFiles: String, conf: String, jars: String, base64EncodedArguments: String, jobName: String, fileSize: Long): String = {
        val arguments = Array(base64EncodedArguments)

        val sparkSubmitJobDriver = {
            val sparkSubmitParameters = {
                val parameters = new StringBuilder()

                if(className != null)
                    parameters.append("--class " + className + " ")
                if(pyFiles != null)
                    parameters.append("--py-files " + pyFiles + " ")
                if(conf != null)
                    parameters.append("--conf " + conf + " ")
                if(jars != null)
                    parameters.append("--jars " + jars + " ")
                logger.info("Running spark job: " + jobName + " with the following parameters: --file " + file + " " + parameters.mkString)

                val driverMemory = PipelineEnvironment.values.sparkProperties.jobConfiguration.driverMemory
                val executorMemory = PipelineEnvironment.values.sparkProperties.jobConfiguration.executorMemory
                val numExecutors = PipelineEnvironment.values.sparkProperties.jobConfiguration.numExecutors
                val executorCores = PipelineEnvironment.values.sparkProperties.jobConfiguration.executorCores

                parameters.append("--conf spark.driver.memory=" + driverMemory + " --conf spark.executor.memory=" + executorMemory + " --conf spark.executor.instances=" + numExecutors + " --conf spark.executor.cores=" + executorCores + " ")
                parameters.mkString
            }

            if (sparkSubmitParameters != null) {
                new SparkSubmitJobDriver()
                    .withEntryPoint(file)
                    .withEntryPointArguments(arguments.toList.asJava)
                    .withSparkSubmitParameters(sparkSubmitParameters)
            }
            else {
                new SparkSubmitJobDriver()
                    .withEntryPoint(file)
                    .withEntryPointArguments(arguments.toList.asJava)
            }
        }
        val jobDriver = new JobDriver().withSparkSubmitJobDriver(sparkSubmitJobDriver)

        // Retrieve the EMR configuration values from S3
        val json = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(PipelineEnvironment.values.sparkProperties.eksEmrProperties.configurationFileUrl),
            ObjectStoreUtil.getKey(PipelineEnvironment.values.sparkProperties.eksEmrProperties.configurationFileUrl))
            .getOrElse(throw new PipelineException("EKS EMR configuration file was not found at the url: " + PipelineEnvironment.values.sparkProperties.eksEmrProperties.configurationFileUrl))
        val gson = new Gson
        val applicationConfiguration = gson.fromJson(json, classOf[java.util.List[Configuration]])
        val s3MonitoringConfiguration = new S3MonitoringConfiguration().withLogUri(PipelineEnvironment.values.sparkProperties.eksEmrProperties.monitoringLogUri)
        val cloudWatchMonitoringConfiguration = new CloudWatchMonitoringConfiguration()
            .withLogGroupName(PipelineEnvironment.values.sparkProperties.eksEmrProperties.cloudWatchLogGroupName)
            .withLogStreamNamePrefix(jobName)
        val monitoringConfiguration = new MonitoringConfiguration()
            .withS3MonitoringConfiguration(s3MonitoringConfiguration)
            .withCloudWatchMonitoringConfiguration(cloudWatchMonitoringConfiguration)
        val configurationOverrides = new ConfigurationOverrides()
            .withMonitoringConfiguration(monitoringConfiguration)
            .withApplicationConfiguration(applicationConfiguration)

        val startJobRunRequest = new StartJobRunRequest()
            .withName(jobName)
            .withVirtualClusterId(PipelineEnvironment.values.sparkProperties.eksEmrProperties.virtualClusterId)
            .withJobDriver(jobDriver)
            .withExecutionRoleArn(PipelineEnvironment.values.sparkProperties.eksEmrProperties.executionRoleArn)
            .withReleaseLabel(PipelineEnvironment.values.sparkProperties.eksEmrProperties.releaseLabel)
            .withConfigurationOverrides(configurationOverrides)
        val emrClient = AmazonEMRContainersClientBuilder.standard().build()
        val sparkJobRunResult = emrClient.startJobRun(startJobRunRequest)
        sparkJobRunResult.getId
    }

    def getSparkJobStatus(jobId: String): Long = {
        val emrClient = AmazonEMRContainersClientBuilder.standard().build()

        val describeJobRunRequest = new DescribeJobRunRequest()
            .withVirtualClusterId(PipelineEnvironment.values.sparkProperties.eksEmrProperties.virtualClusterId)
            .withId(jobId)
        val describeJobRunResult = emrClient.describeJobRun(describeJobRunRequest)
        val jobRun = describeJobRunResult.getJobRun
        val state = {
            jobRun.getState match {
                case "PENDING" | "SUBMITTED" | "RUNNING" => SparkJobStatus.RUNNING
                case "COMPLETED" => SparkJobStatus.SUCCESS
                case "FAILED" | "CANCELLED" | "CANCEL_PENDING" => SparkJobStatus.DEAD
                case _ => SparkJobStatus.DEAD
            }
        }
        state
    }
}
