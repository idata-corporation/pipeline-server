package net.idata.pipeline.transform.util

import net.idata.pipeline.common.model.PipelineException
import net.idata.pipeline.common.util.ObjectStoreUtil
import net.idata.pipeline.transform.Transform
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.util
import scala.collection.JavaConverters._

object JsonXmlReader {
    private val sparkSession = Transform.sparkSession

    def readFile(url: String, everyRowContainsObject: Boolean, columnName: String): DataFrame = {
        val schema = StructType(Array(StructField(columnName, StringType, nullable = false)))

        if(everyRowContainsObject) {
            val rows = new util.ArrayList[Row]()

            // Each line in the file is a JSON object
            val (reader, s3Object) = ObjectStoreUtil.getBufferedReader(ObjectStoreUtil.getBucket(url), ObjectStoreUtil.getKey(url))

            var line = reader.readLine()
            while(line != null && line.nonEmpty) {
                rows.add(Row(line))
                line = reader.readLine()
            }
            s3Object.close()

            val rowsRdd = sparkSession.sparkContext.parallelize(rows.asScala)
            sparkSession.createDataFrame(rowsRdd, schema)
        }
        else {
            // Read the entire file as 1 row with 1 column
            val json = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(url), ObjectStoreUtil.getKey(url)).getOrElse(
                throw new PipelineException("Error reading the raw file: " + url))

            val rowsRdd = sparkSession.sparkContext.parallelize(Seq(Row(json)))
            sparkSession.createDataFrame(rowsRdd, schema)
        }
    }
}