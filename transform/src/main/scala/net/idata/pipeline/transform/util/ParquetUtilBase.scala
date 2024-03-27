package net.idata.pipeline.transform.util

import com.google.common.base.Throwables
import net.idata.pipeline.common.model.{DatasetConfig, ObjectStore, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, ObjectStoreSQLUtil, StatusUtil}
import net.idata.pipeline.transform.Transform

import java.time.Instant
import scala.util.control.Breaks.{break, breakable}

class ParquetUtilBase(config: DatasetConfig) {
    val objectStore: ObjectStore = config.destination.objectStore
    val statusUtil: StatusUtil = Transform.statusUtil

    def repairTable(): Unit = {
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
