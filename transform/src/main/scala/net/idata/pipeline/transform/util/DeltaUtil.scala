package net.idata.pipeline.transform.util

import io.delta.tables.DeltaTable
import net.idata.pipeline.common.model.{DatasetConfig, PipelineException}
import net.idata.pipeline.common.util.{ObjectStoreUtil, StatusUtil}
import net.idata.pipeline.transform.Transform.sparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

class DeltaUtil(config: DatasetConfig) extends ParquetUtilBase(config) {
    def write(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        statusUtil.info("processing", "Writing dataset using Delta")

        // Do merge?
        if(objectStore.keyFields != null && !objectStore.keyFields.isEmpty) {
            if(!deltaDataExists(destinationUrl))
                writeDeltaFirstRow(dfUpdates, destinationUrl)
            writeDeltaMerge(dfUpdates, destinationUrl)
        }
        else {
            val writeMode = {
                if(objectStore.sparkWriteMode != null && objectStore.sparkWriteMode.compareToIgnoreCase("overwrite") == 0)
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
        if(objectStore.keyFields == null || objectStore.keyFields.size == 0)
            throw new PipelineException("The dataset " + config.name + " config 'destination.objectStore.useDelta' is true, but there are no keyFields defined for the merge")

        // Upsert into the delta table
        val deltaTable = DeltaTable.forPath(sparkSession, destinationUrl + "/")
        val condition = objectStore.keyFields.asScala.map(keyField => {
            config.name + "." + keyField + " <=> " + "updates." + keyField
        }).toList.mkString(" AND ")
        StatusUtil.info("processing", "delta condition: " + condition)

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
        if(objectStore.partitionBy != null && objectStore.partitionBy.size > 0) {
            dfUpdates.write
                .format("delta")
                .mode(mode)
                .partitionBy(objectStore.partitionBy.asScala:_*)
                .save(destinationUrl)
        }
        else {
            dfUpdates.write
                .format("delta")
                .mode(mode)
                .save(destinationUrl)
        }
    }

    private def deltaDataExists(destinationUrl: String): Boolean = {
        try {
            ObjectStoreUtil.keyExists(ObjectStoreUtil.getBucket(destinationUrl), ObjectStoreUtil.getKey(destinationUrl))
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

        if(objectStore.partitionBy != null && objectStore.partitionBy.size > 0)
            repairTable()
    }
}
