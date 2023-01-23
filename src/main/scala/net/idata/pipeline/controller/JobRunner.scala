package net.idata.pipeline.controller

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import com.google.common.base.Throwables
import net.idata.pipeline.model.{PipelineException, JobContext}
import net.idata.pipeline.util.{DataQuality, ObjectStoreLoader, RedshiftLoader, SnowflakeLoader}
import org.slf4j.{Logger, LoggerFactory}

class JobRunner(jobContext: JobContext) extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[JobRunner])

    def run(): Unit = {
        val config = jobContext.config
        val statusUtil = jobContext.statusUtil

        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        try {
            statusUtil.info("begin", "Process started")

            // Do data quality?
            val jobContextDQ = {
                if(config.dataQuality != null)
                    new DataQuality(jobContext).process()
                else
                    jobContext
            }

            // Transformations?
            val jobContextTransform = {
                jobContextDQ
            }

            if(config.destination.objectStore != null)
                new ObjectStoreLoader(jobContextTransform).process()

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
}