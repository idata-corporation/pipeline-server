package net.idata.pipeline.util

import com.google.gson.Gson
import net.idata.pipeline.common.model.PipelineEnvironment
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object LivyUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def executeSparkJob(file: String, className: String, pyFiles: String, conf: String, jars: String, base64EncodedArguments: String, jobName: String, fileSize: Long): String = {
        val url = "http://" + PipelineEnvironment.values.sparkProperties.emrProperties.emrMasterNodeIp + ":8998" + "/batches"

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

        val (sparkDriverMemory, sparkExecutorMemory, sparkNumExecutors, sparkExecutorCores) = {
            if(fileSize <= PipelineEnvironment.values.sparkProperties.emrProperties.smallSparkMemoryCores.maxFileSize) {
                logger.info("Using the small spark memory cores")
                (PipelineEnvironment.values.sparkProperties.emrProperties.smallSparkMemoryCores.sparkDriverMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.smallSparkMemoryCores.sparkExecutorMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.smallSparkMemoryCores.sparkNumExecutors,
                    PipelineEnvironment.values.sparkProperties.emrProperties.smallSparkMemoryCores.sparkExecutorCores)
            }
            else if(fileSize <= PipelineEnvironment.values.sparkProperties.emrProperties.mediumSparkMemoryCores.maxFileSize) {
                logger.info("Using the medium spark memory cores")
                (PipelineEnvironment.values.sparkProperties.emrProperties.mediumSparkMemoryCores.sparkDriverMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.mediumSparkMemoryCores.sparkExecutorMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.mediumSparkMemoryCores.sparkNumExecutors,
                    PipelineEnvironment.values.sparkProperties.emrProperties.mediumSparkMemoryCores.sparkExecutorCores)
            }
            else {
                logger.info("Using the large spark memory cores")
                (PipelineEnvironment.values.sparkProperties.emrProperties.largeSparkMemoryCores.sparkDriverMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.largeSparkMemoryCores.sparkExecutorMemory,
                    PipelineEnvironment.values.sparkProperties.emrProperties.largeSparkMemoryCores.sparkNumExecutors,
                    PipelineEnvironment.values.sparkProperties.emrProperties.largeSparkMemoryCores.sparkExecutorCores)
            }
        }

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
            sparkDriverMemory,
            sparkExecutorMemory,
            sparkExecutorCores.toInt,
            sparkNumExecutors.toInt)

        val gson = new Gson
        val jsonToPost = gson.toJson(payload)
        val response = HttpUtil.post(url, "application/json", jsonToPost, bearerToken = null, 15000, retry = true, retryWaitMillis = 10000)
        val sparkExecutorResult = gson.fromJson(response.mkString, classOf[EMRSparkExecutorResult])
        sparkExecutorResult.id.toString
    }

    def getSparkJobStatus(jobId: String): Long = {
        val masterNodeIP = PipelineEnvironment.values.sparkProperties.emrProperties.emrMasterNodeIp
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
