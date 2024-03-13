package net.idata.pipeline.transform.util

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
import net.idata.pipeline.common.model._
import net.idata.pipeline.common.util._
import net.idata.pipeline.transform.Transform
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

class ObjectStoreUtil(config: DatasetConfig) {
    private val objectStore = config.destination.objectStore
    private val statusUtil = Transform.statusUtil

    def write(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        if(objectStore.useIceberg)
            writeIceberg(dfUpdates)
        else
            writeParquet(dfUpdates, destinationUrl)
    }

    private def writeParquet(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        statusUtil.info("processing", "Writing dataset to objectStore")
        val saveMode = {
            if(config.destination.objectStore.sparkWriteMode != null && config.destination.objectStore.sparkWriteMode.compareToIgnoreCase("overwrite") == 0)
                SaveMode.Overwrite
            else
                SaveMode.Append
        }

        // Write out as parquet
        if(objectStore.partitionBy == null || objectStore.partitionBy.size == 0) {
            dfUpdates.write
                .mode(saveMode)
                .parquet(destinationUrl)
        } else {
            dfUpdates.write
                .mode(saveMode)
                .partitionBy(objectStore.partitionBy.asScala:_*)
                .parquet(destinationUrl)
            repairTable(config)
        }
    }

    private def writeIceberg(dfUpdates: DataFrame): Unit = {
        statusUtil.info("processing", "Writing dataset using Iceberg")

        val tablePath = config.source.schemaProperties.dbName + "." + config.name
        dfUpdates.writeTo(tablePath).createOrReplace()

        repairTable(config)
        /*
        TODO - Do MERGE INTO when the Apache API supports it
        MERGE INTO prod.db.target t -- a target table
        USING (SELECT ...) s        -- the source updates
        ON t.id = s.id              -- condition to find updates for target rows
        WHEN MATCHED AND <predicate_x> THEN DELETE
        WHEN MATCHED AND <predicate_y> THEN UPDATE *
        WHEN NOT MATCHED THEN INSERT *


        MERGE INTO prod.db.table.branch_audit t
        USING (SELECT ...) s
        ON t.id = s.id
        WHEN ...

         */
    }

    private def repairTable(config: DatasetConfig): Unit = {
        println("repairing table")

        // Call Athena to repair the table
        val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
        statusUtil.info("processing", "MSK REPAIR TABLE " + config.name + ", Athena output path: " + outputPath)

        // Repair the partition with a retry
        breakable {
            val waitTime = 10000
            for (n <- 1 to 4) {
                try {
                    ObjectStoreSQLUtil.sql(config.destination.schemaProperties.dbName, "MSCK REPAIR TABLE " + config.name, outputPath)
                    break
                } catch {
                    case e: Exception =>
                        if (n == 4) {
                            // Changed from throwing an PipelineException to the following.  A repair table is not necessarily the sign of a completely failed job
                            statusUtil.error("processing", "Error repairing the partition: " + Throwables.getStackTraceAsString(e))
                            throw new PipelineException(Throwables.getStackTraceAsString(e))
                        }
                        else
                            Thread.sleep(waitTime * n)
                }
            }
        }
    }
}