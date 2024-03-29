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

import com.google.gson.Gson
import net.idata.pipeline.common.model.PipelineEnvironment
import net.idata.pipeline.model.spark.{EMRSparkExecutorResult, EMRSparkJobStatus, LivyPayload, SparkJobStatus}
import net.idata.pipeline.util.HttpUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object LivyUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def executeSparkJob(file: String, className: String, pyFiles: String, conf: String, jars: String, base64EncodedArguments: String, jobName: String, fileSize: Long): String = {
        val url = "http://" + PipelineEnvironment.values.sparkProperties.emrProperties.masterNodeIp + ":8998" + "/batches"

        // Base64 encoded arguments are required because of a bug in YARN with 2 brackets }} - https://issues.apache.org/jira/browse/SPARK-17814
        val arguments = Array(base64EncodedArguments)

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

        val myJars = {
            if(jars != null)
                jars.split(",").toList.asJava
            else
                null
        }
        val myConf = {
            if(conf != null) {
                val nameValue =conf.split(",")
                nameValue.map(_.split("=")).map(arr => arr(0) -> arr(1)).toMap.asJava
            } else
                null
        }
        val myPyFiles = {
            if(pyFiles != null)
                pyFiles.split(",").toList.asJava
            else
                null
        }

        val payload = LivyPayload(
            file,
            className,
            arguments,
            myJars,
            myConf,
            myPyFiles,
            driverMemory,
            executorMemory,
            executorCores,
            numExecutors)

        val gson = new Gson
        val jsonToPost = gson.toJson(payload)
        logger.info("jsonToPost: " + jsonToPost)
        val response = HttpUtil.post(url, "application/json", jsonToPost, bearerToken = null, 15000, retry = true, retryWaitMillis = 10000)
        val sparkExecutorResult = gson.fromJson(response.mkString, classOf[EMRSparkExecutorResult])
        sparkExecutorResult.id.toString
    }

    def getSparkJobStatus(jobId: String): Long = {
        val masterNodeIP = PipelineEnvironment.values.sparkProperties.emrProperties.masterNodeIp
        val url = "http://" + masterNodeIP + ":8998" + "/batches/" + jobId
        val response = HttpUtil.get(url, bearerToken = null, 15000, retry = true, retryWaitMillis = 60000)

        val gson = new Gson
        val emrSparkJobStatus = gson.fromJson(response, classOf[EMRSparkJobStatus])

        emrSparkJobStatus.state match {
            case "success" => SparkJobStatus.SUCCESS
            case "dead" => SparkJobStatus.DEAD
            case _ => SparkJobStatus.RUNNING
        }
    }
}
