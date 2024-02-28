package net.idata.pipeline.transform.util

import com.google.common.base.Throwables
import net.idata.pipeline.common.model._
import net.idata.pipeline.common.util._
import net.idata.pipeline.transform.Transform
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

class ObjectStoreUtil(config: DatasetConfig) {
    private val sparkSession = Transform.sparkSession
    private val objectStoreAttributes = config.destination.objectStore
    private val statusUtil = Transform.statusUtil

    def write(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        //if(objectStoreAttributes.useIceberg)
        //    writeIceberg(dfUpdates, destinationUrl)
        //else
            writeNormalAppend(dfUpdates, destinationUrl)
    }

    /*
    private def writeIceberg(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        statusUtil.info("processing", "Writing dataset using Iceberg")

        // Do merge?
        if(objectStoreAttributes.keyFields != null && !objectStoreAttributes.keyFields.isEmpty) {
            if(!deltaDataExists(destinationUrl))
                writeDeltaFirstRow(dfUpdates, destinationUrl)
            writeDeltaMerge(dfUpdates, destinationUrl)
        }
        else {
            val writeMode = {
                if(objectStoreAttributes.writeMode != null && objectStoreAttributes.writeMode.compareToIgnoreCase("overwrite") == 0)
                    "overwrite"
                else
                    "append"
            }
            writeDeltaWithMode(dfUpdates, destinationUrl, writeMode)
        }

        // When the data in a Delta table is updated, you must regenerate the manifests
        generateManifests(destinationUrl, config)
    }

    private def writeDeltaFirstRow(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        val firstRow = dfUpdates.first
        val rowsRdd: RDD[Row] = sparkSession.sparkContext.parallelize(Seq(firstRow))
        val dfFirstRow = sparkSession.createDataFrame(rowsRdd, dfUpdates.schema)

        if(config.destination.objectStore.partitionBy != null && config.destination.objectStore.partitionBy.size > 0) {
            dfFirstRow.write
                .format("delta")
                .mode("append")
                .partitionBy(config.destination.objectStore.partitionBy.asScala:_*)
                .save(destinationUrl)
        }
        else {
            dfFirstRow.write
                .format("delta")
                .mode("append")
                .save(destinationUrl)
        }
    }

    private def writeDeltaMerge(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        // Determine the condition for updates
        if(objectStoreAttributes.keyFields == null || objectStoreAttributes.keyFields.size == 0)
            throw new PipelineException("The dataset " + config.name + " config 'destination.objectStore.useDelta' is true, but there are no keyFields defined for the merge")

        // Upsert into the delta table
        val deltaTable = DeltaTable.forPath(sparkSession, destinationUrl + "/")
        val condition = objectStoreAttributes.keyFields.asScala.map(keyField => {
            config.name + "." + keyField + " <=> " + "updates." + keyField
        }).toList.mkString(" AND ")
        statusUtil.info("processing", "info", "delta condition: " + condition)

        // Write the table with a retry
        breakable {
            val waitTime = 5000
            for(n <- 1 to 4){
                try {
                    deltaTable.as(config.name)
                        .merge(dfUpdates.as("updates"), condition)
                        .whenMatched.updateAll()
                        .whenNotMatched.insertAll()
                        .execute()
                    break
                } catch {
                    case e: Exception =>
                        if(n == 4)
                            throw new PipelineException("Error writing Delta table: " + e.getMessage)
                        else
                            Thread.sleep(waitTime * n)
                }
            }
        }
    }

    private def writeDeltaWithMode(dfUpdates: DataFrame, destinationUrl: String, mode: String): Unit = {
        // if the delta table does not exist, do an append
        if(objectStoreAttributes.partitionBy != null && objectStoreAttributes.partitionBy.size > 0) {
            dfUpdates.write
                .format("delta")
                .mode(mode)
                .partitionBy(objectStoreAttributes.partitionBy.asScala:_*)
                .save(destinationUrl)
        }
        else {
            dfUpdates.write
                .format("delta")
                .mode(mode)
                .save(destinationUrl)
        }
    }
    */

    private def writeNormalAppend(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        statusUtil.info("processing", "Writing dataset to objectStore")
        val saveMode = {
            if(config.destination.objectStore.sparkWriteMode != null && config.destination.objectStore.sparkWriteMode.compareToIgnoreCase("overwrite") == 0)
                SaveMode.Overwrite
            else
                SaveMode.Append
        }

        // Write out as parquet
        if(objectStoreAttributes.partitionBy == null || objectStoreAttributes.partitionBy.size == 0) {
            dfUpdates.write
                .mode(saveMode)
                .parquet(destinationUrl)
        } else {
            dfUpdates.write
                .mode(saveMode)
                .partitionBy(objectStoreAttributes.partitionBy.asScala:_*)
                .parquet(destinationUrl)
            repairTable(config)
        }
    }

    /*
    private def deltaDataExists(destinationUrl: String): Boolean = {
        try {
            ObjectStoreUtil.keysExist(ObjectStoreUtil.getBucket(destinationUrl), ObjectStoreUtil.getKey(destinationUrl))
        }
        catch {
            case e: Exception =>
                false
        }
    }

    private def generateManifests(destinationUrl: String, config: DatasetConfig): Unit = {
        // Generate the manifests
        val deltaTable = DeltaTable.forPath(sparkSession, destinationUrl + "/")
        deltaTable.generate("symlink_format_manifest")

        if(objectStoreAttributes.partitionBy != null && objectStoreAttributes.partitionBy.size > 0)
            repairTable(config)
    }
     */

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