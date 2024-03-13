package net.idata.pipeline.transform

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
import com.google.gson.GsonBuilder
import net.idata.pipeline.common.model._
import net.idata.pipeline.common.model.spark.SparkRuntime
import net.idata.pipeline.common.util.{DatasetConfigIO, StatusUtil}
import net.idata.pipeline.transform.util._
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Base64

object Transform {
    private val processName = "Transform"
    var sparkSession: SparkSession = _
    var statusUtil: StatusUtil = _

    def main(args: Array[String]): Unit = {
        var exitCode = 0

        try {
            println("Spark Transform application started successfully")

            // Grab the arguments
            val arguments = new String(Base64.getDecoder.decode(args(0)), StandardCharsets.UTF_8)
            println("Arguments decoded: " + arguments)
            val gson = new GsonBuilder().disableHtmlEscaping.create
            val sparkRuntime = gson.fromJson(arguments, classOf[SparkRuntime])

            // Initialize the Spark session
            sparkSession = initSparkSession(sparkRuntime.useIceberg)

            PipelineEnvironment.init(sparkRuntime.pipelineEnvironment)

            // Initialize the status utility
            statusUtil = new StatusUtil().init(PipelineEnvironment.values.datasetStatusTableName, this.getClass.getSimpleName)
            statusUtil.init(sparkRuntime.pipelineEnvironment.datasetStatusTableName, processName)
            statusUtil.setPipelineToken(sparkRuntime.pipelineToken)
            statusUtil.setPublisherToken(sparkRuntime.publisherToken)
            statusUtil.setFilename(sparkRuntime.metadata)
            statusUtil.info("begin", "Process started")

            //sparkSession = initSparkSession()
            statusUtil.info("processing", "applicationID: " + sparkSession.sparkContext.applicationId)

            process(sparkRuntime)

            println("Process completed successfully")
            statusUtil.info("end", "Process completed")
        } catch {
            case e: Exception =>
                println(Throwables.getStackTraceAsString(e))
                StatusUtil.error("end", "Process failed with error: " + Throwables.getStackTraceAsString(e))
                exitCode = -1
        } finally {
            sparkSession.close()
        }

        System.exit(exitCode)
    }

    private def initSparkSession(useIceberg: Boolean): SparkSession = {
        val warehouseLocation = new File("spark-warehouse").getAbsolutePath

        if(useIceberg) {
            SparkSession
                .builder()
                .appName(processName)
                .config("spark.sql.warehouse.dir", warehouseLocation)           // Needed for repairing partitions via Hive
                // Databricks Delta configurations
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.glue_catalog.warehouse", warehouseLocation)
                .config("spark.sql.catalog.glue_catalog.catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .enableHiveSupport()
                .getOrCreate
        }
        else {
            SparkSession
                .builder()
                .appName(processName)
                .config("spark.sql.warehouse.dir", warehouseLocation)           // Needed for repairing partitions via Hive
                .enableHiveSupport()
                .getOrCreate
        }
    }

    private def process(sparkRuntime: SparkRuntime): Unit = {
        val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, sparkRuntime.datasetName)
        println("config: " + config.toString)

        // Read data into a dataframe
        val dataFrameUtil = new DataFrameUtil(config)
        val dfSource = dataFrameUtil.readData(sparkRuntime.sourceTransformUrl)
        //dfSource.show(100, truncate = false)   // TODO: Only for debugging

        val dfTransformed = {
            // If not unstructured and not semi-structured validate and transform the data
            val unstructured = config.source.fileAttributes.unstructuredAttributes != null
            val semiStructured = config.source.fileAttributes.jsonAttributes != null || config.source.fileAttributes.xmlAttributes != null
            if(!semiStructured && !unstructured) {
                // Ensure the headers are lowercase.  If there is no source schema, they can be mixed
                val dfHeadersLowerCase = dfSource.toDF(dfSource.columns map(_.toLowerCase): _*)

                // Apply the destination schema
                dataFrameUtil.applyDestinationSchema(dfHeadersLowerCase)
            }
            else
                dfSource
        }
        //println("dfTransformed")
        //dfTransformed.show(100, truncate = false)   // TODO: Only for debugging

        // Write to objectStore?
        val objectStoreUtil = new ObjectStoreUtil(config)
        objectStoreUtil.write(dfTransformed, sparkRuntime.destinationTransformUrl)

        // Write to a temp location for the REST API GET /dataset/data?pipelinetoken=?
        if(config.destination.objectStore.writeToTemporaryLocation)
            dataFrameUtil.writeToTemporaryForRestAPI(dfTransformed)
    }
}