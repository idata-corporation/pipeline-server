package net.idata.pipeline.controller

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.model.{JobContext, JobState}
import net.idata.pipeline.util.EksEmrUtil

import java.nio.charset.StandardCharsets
import java.util.Base64

class SparkController {
    def startJob(jobContext: JobContext): String = {
        jobContext.statusUtil.overrideProcessName(this.getClass.getSimpleName)
        try {
            val jobId = {
                if(PipelineEnvironment.values.sparkProperties.useEmr)
                    useEmr(jobContext)
                else if(PipelineEnvironment.values.sparkProperties.useEksEmr)
                    useEksEmr(jobContext)
                else
                    throw new PipelineException("PipelineEnvironment is not setup properly for the spark engine.  Must declare one of the values as 'true'. useEmr or useEksEmr")
            }

            jobContext.statusUtil.info("end", "Process completed successfully")
            jobId
        }
        catch {
            case e: Exception =>
                throw new PipelineException(Throwables.getStackTraceAsString(e))
        }
    }

    def getJobStatus(jobId: String): Long = {
        if(PipelineEnvironment.values.sparkProperties.useEmr)
            LivyUtil.getSparkJobStatus(jobId)
        else if(PipelineEnvironment.values.sparkProperties.useEksEmr)
            EksEmrUtil.getSparkJobStatus(jobId)
        else
            throw new PipelineException("PipelineEnvironment is not setup properly for the spark engine.  Must declare one of the values as 'true'.  useEmr, useEksEmr, or useDatabricks")
    }

    private def useEmr(jobContext: JobContext): String = {
        val gson = new Gson
        val base64EncodedArguments = Base64.getEncoder.encodeToString(gson.toJson(jobContext.datasetProperties).getBytes(StandardCharsets.UTF_8))

        jobContext.statusUtil.info("processing", "Launch a spark job, file: " + jobContext.datasetProperties.transformFile + ", class: " + jobContext.datasetProperties.transformClassName)
        val jobName = jobContext.datasetProperties.transformClassName.split("[.]").last
        val jobId = LivyUtil.executeSparkJob(jobContext.datasetProperties.transformFile, jobContext.datasetProperties.transformClassName, null, null, null, base64EncodedArguments, jobName, jobContext.data.size)

        jobContext.statusUtil.info("processing", "EMR job started with ID: " + jobId)
        jobId
    }

    private def useEksEmr(jobContext: JobContext): String = {
        val gson = new Gson
        val base64EncodedArguments = Base64.getEncoder.encodeToString(gson.toJson(jobContext.datasetProperties).getBytes(StandardCharsets.UTF_8))

        jobContext.statusUtil.info("processing", "Launch a spark job, file: " + jobContext.datasetProperties.transformFile + ", class: " + jobContext.datasetProperties.transformClassName)
        val jobName = jobContext.datasetProperties.transformClassName.split("[.]").last
        val jobId = EksEmrUtil.executeSparkJob(jobContext.datasetProperties.transformFile,
            jobContext.datasetProperties.transformClassName,
            null,
            null,
            null,
            base64EncodedArguments,
            jobName + "-" + jobContext.config.name,
            jobContext.data.size)

        jobContext.statusUtil.info("processing", "EKS EMR job started with ID: " + jobId)
        jobId
    }
}