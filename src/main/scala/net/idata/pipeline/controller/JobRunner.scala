package net.idata.pipeline.controller

/*
IData Pipeline
Copyright (C) 2023 IData Corporation (http://www.idata.net)

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

Author(s): Todd Fearn
*/

import com.google.common.base.Throwables
import net.idata.pipeline.model.{JobContext, PipelineException}
import net.idata.pipeline.util._

class JobRunner(jobContext: JobContext) extends Runnable {
    def run(): Unit = {
        val config = jobContext.config
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