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
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, ObjectStoreUtil}
import net.idata.pipeline.model.{GlobalJobContext, JobContext}
import net.idata.pipeline.util._

import java.time.Instant

class JobRunner(jobContext: JobContext) extends Runnable {
    private val config = jobContext.config

    def run(): Unit = {
        val statusUtil = jobContext.statusUtil

        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        try {
            statusUtil.info("begin", "Process started")

            // Do data quality?
            if(config.dataQuality != null)
                new DataQuality(jobContext).process()

            // Transformations?
            val jobContextTransform = {
                if(config.transformation != null)
                    new Transformation(jobContext).process()
                else
                    jobContext
            }

            if(config.destination.objectStore != null) {
                // Default to Athena, unless useSparkCluster is set
                val useSparkCluster = config.destination.objectStore.useSparkCluster

                if(useSparkCluster) {
                    val newJobContext = createSourceFileForSpark(jobContext)

                    val jobId = new SparkController().startJob(newJobContext)
                    GlobalJobContext.replaceJobContext(newJobContext.copy(sparkJobId = jobId))
                }
                else
                    new ObjectStoreLoader(jobContextTransform).process()
            }

            if(config.destination.database != null) {
                if(config.destination.database.snowflake != null)
                    new SnowflakeLoader(jobContextTransform).process()

                if(config.destination.database.redshift != null)
                    new RedshiftLoader(jobContextTransform).process()
            }

            statusUtil.info("end", "Process completed successfully")
        } catch {
            case e: Exception =>
                statusUtil.error("end","Process completed, error: " + Throwables.getStackTraceAsString(e))
                throw new PipelineException("Pipeline error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def createSourceFileForSpark(jobContext: JobContext): JobContext = {
        // If using a Spark cluster for transformation to parquet, if the data has been transformed,
        // create a new source file for the Spark cluster to use
        if(config.transformation != null) {
            val sourceUrl = "s3://" + PipelineEnvironment.values.environment + "-temp/spark/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
            ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(sourceUrl), ObjectStoreUtil.getKey(sourceUrl), jobContext.data.rawData)

            val sparkRuntime = jobContext.sparkRuntime.copy(sourceTransformUrl = sourceUrl)
            jobContext.copy(sparkRuntime = sparkRuntime)
        }
        else
            jobContext
    }
}