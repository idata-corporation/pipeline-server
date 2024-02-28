package net.idata.pipeline.transform

import com.google.common.base.Throwables
import com.google.gson.GsonBuilder
import net.idata.pipeline.common.model._
import net.idata.pipeline.common.util.{DatasetConfigIO, StatusUtil}
import net.idata.pipeline.transform.util._
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Base64

object Transform {
    val pipelineDateFormat = "yyyy-MM-dd HH:mm:ss.SSS z"
    val eventualConsistencySleepTime = 15000
    val processName = "Transform"
    var sparkSession: SparkSession = initSparkSession()
    var statusUtil: StatusUtil = _

    def main(args: Array[String]): Unit = {
        var exitCode = 0

        try {
            println("Spark Transform application started successfully")

            // Grab the arguments
            val arguments = new String(Base64.getDecoder.decode(args(0)), StandardCharsets.UTF_8)
            println("Arguments decoded: " + arguments)
            val gson = new GsonBuilder().disableHtmlEscaping.create
            val properties = gson.fromJson(arguments, classOf[DatasetProperties])

            PipelineEnvironment.init(properties.pipelineEnvironment)

            // Initialize the status utility
            statusUtil = new StatusUtil().init(PipelineEnvironment.values.datasetStatusTableName, this.getClass.getSimpleName)
            statusUtil.init(properties.pipelineEnvironment.datasetStatusTableName, processName)
            statusUtil.setPipelineToken(properties.pipelineToken)
            statusUtil.setPublisherToken(properties.publisherToken)
            statusUtil.setFilename(properties.metadata)
            statusUtil.info("begin", "Process started")

            //sparkSession = initSparkSession()
            statusUtil.info("processing", "applicationID: " + sparkSession.sparkContext.applicationId)

            process(properties)

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

    private def initSparkSession(): SparkSession = {
        val warehouseLocation = new File("spark-warehouse").getAbsolutePath
        SparkSession
            .builder()
            .appName(processName)
            .config("spark.sql.warehouse.dir", warehouseLocation)           // Needed for repairing partitions via Hive
            .enableHiveSupport()
            .getOrCreate
    }

    private def process(properties: DatasetProperties): Unit = {
        val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, properties.name)
        println("config: " + config.toString)

        // Read data into a dataframe
        val dataFrameUtil = new DataFrameUtil(properties, config)
        val dfSource = dataFrameUtil.readData(properties.sourceTransformUrl)
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
        val destinationTemporaryUrl = {
            if(config.destination.objectStore != null) {
                val objectStoreUtil = new ObjectStoreUtil(config)
                objectStoreUtil.write(dfTransformed, properties.destinationTransformUrl)

                // Write to a temp location for the REST API GET /dataset/data?pipelinetoken=?
                if(config.destination.objectStore.writeToTemporaryLocation)
                    dataFrameUtil.writeToTemporaryForRestAPI(dfTransformed)
                else
                    null
            }
            else
                null
        }
    }
}