package net.idata.pipeline.controller

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

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.model.JobContext
import net.idata.pipeline.util.BuildInfoUtil
import net.idata.pipeline.util.spark.{EksEmrUtil, LivyUtil}

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
        val base64EncodedArguments = Base64.getEncoder.encodeToString(gson.toJson(jobContext.sparkRuntime).getBytes(StandardCharsets.UTF_8))

        val (transformFile, transformClassName) = BuildInfoUtil.getTransformInfo(PipelineEnvironment.values.environment)
        jobContext.statusUtil.info("processing", "Launch a spark job, file: " + transformFile + ", class: " + transformClassName)
        val jobName = transformClassName.split("[.]").last
        val jobId = LivyUtil.executeSparkJob(transformFile, transformClassName, null, null, null, base64EncodedArguments, jobName, jobContext.data.size)

        jobContext.statusUtil.info("processing", "EMR job started with ID: " + jobId)
        jobId
    }

    private def useEksEmr(jobContext: JobContext): String = {
        val gson = new Gson
        val base64EncodedArguments = Base64.getEncoder.encodeToString(gson.toJson(jobContext.sparkRuntime).getBytes(StandardCharsets.UTF_8))

        val (transformFile, transformClassName) = BuildInfoUtil.getTransformInfo(PipelineEnvironment.values.environment)
        jobContext.statusUtil.info("processing", "Launch a spark job, file: " + transformFile + ", class: " + transformClassName)
        val jobName = transformClassName.split("[.]").last
        val jobId = EksEmrUtil.executeSparkJob(transformFile,
            transformClassName,
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