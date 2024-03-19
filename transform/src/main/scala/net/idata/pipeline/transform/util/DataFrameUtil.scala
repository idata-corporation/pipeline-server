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

import net.idata.pipeline.common.model._
import net.idata.pipeline.common.util.aws.GlueUtil
import net.idata.pipeline.transform.Transform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable

class DataFrameUtil(properties: DatasetProperties, config: DatasetConfig) {
    private val sparkSession = Transform.sparkSession

    def readData(url: String): DataFrame = {
        val schema = getSparkSourceSchema(config)

        val fileAttributes = config.source.fileAttributes
        if(fileAttributes.csvAttributes != null) {
            val csvOptions = {
                val options = new mutable.HashMap[String, String]()
                options.put("header", fileAttributes.csvAttributes.header.toString)
                options.put("delimiter", fileAttributes.csvAttributes.delimiter)
                options.put("multiline", "true")
                options.put("escape", "\"")
                if(schema == null) {
                    options.put("inferSchema", "true")
                    options.put("samplingRatio", "1.0")
                }
                if (fileAttributes.csvAttributes.encoding != null && fileAttributes.csvAttributes.encoding.nonEmpty)
                    options.put("encoding", fileAttributes.csvAttributes.encoding)
                if (fileAttributes.sparkReadOptions != null) {
                    fileAttributes.sparkReadOptions.asScala.foreach(option => {
                        options.put(option._1, option._2)
                    })
                }
                options
            }
            if(schema == null)
                sparkSession.read.format("csv").options(csvOptions).load(url)
            else
                sparkSession.read.format("csv").options(csvOptions).schema(schema).load(url)
        }
        else if(fileAttributes.jsonAttributes != null) {
            val columnName = config.destination.schemaProperties.fields.asScala.head.name
            JsonXmlReader.readFile(url, fileAttributes.jsonAttributes.everyRowContainsObject, columnName)
        }
        else if(fileAttributes.xmlAttributes != null) {
            val columnName = config.destination.schemaProperties.fields.asScala.head.name
            JsonXmlReader.readFile(url, fileAttributes.xmlAttributes.everyRowContainsObject, columnName)
        }
        else if(fileAttributes.xlsAttributes != null)
            new ExcelToCsvUtil(properties, config).convertExcelToDataFrame(schema)
        else
            throw new PipelineException("Only csv, json, xml, and xls files can be read by the Spark Transform process")
    }

    def writeToTemporaryForRestAPI(dataFrame: DataFrame): String = {
        val destinationTemporaryUrl = "s3://" + PipelineEnvironment.values.environment + "-temp" + "/data-rest-api/" + config.name + "/" + Instant.now.toEpochMilli.toString

        if(config.source.fileAttributes.csvAttributes != null ||
            config.source.fileAttributes.xlsAttributes != null)
        {
            val delimiter = {
                if(config.source.fileAttributes.csvAttributes != null)
                    config.source.fileAttributes.csvAttributes.delimiter
                else
                    ","  // Use a comma as the default delimiter for files other than CSV
            }

            dataFrame
                .write
                .option("header", "false")
                .option("delimiter", delimiter)
                .csv(destinationTemporaryUrl)
        }
        else if(config.source.fileAttributes.jsonAttributes != null || config.source.fileAttributes.xmlAttributes != null) {
            dataFrame.coalesce(1)
                .write
                .option("header", value = false)
                .text(destinationTemporaryUrl)
        }
        else
            throw new PipelineException("Internal error, fileAttributes not configured properly, error writing temporary file for REST API")

        destinationTemporaryUrl
    }

    def applyDestinationSchema(df: DataFrame): DataFrame = {
        val dfReordered = reorderColumns(df)
        var newDf = dfReordered

        // Use the schema defined in the configuration, not the Glue schema
        config.destination.schemaProperties.fields.forEach(field => {
            if(field.`type`.startsWith("boolean"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(BooleanType).as(field.name))
            else if(field.`type`.startsWith("int"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(IntegerType).as(field.name))
            else if(field.`type`.startsWith("tinyint"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(IntegerType).as(field.name))
            else if(field.`type`.startsWith("smallint"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(IntegerType).as(field.name))
            else if(field.`type`.startsWith("bigint"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(LongType).as(field.name))
            else if(field.`type`.startsWith("float"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(FloatType).as(field.name))
            else if(field.`type`.startsWith("double"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(DoubleType).as(field.name))
            else if(field.`type`.startsWith("decimal"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(DoubleType).as(field.name))
            else if(field.`type`.startsWith("string"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(StringType).as(field.name))
            else if(field.`type`.startsWith("varchar"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(StringType).as(field.name))
            else if(field.`type`.startsWith("char"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(StringType).as(field.name))
            else if(field.`type`.startsWith("date"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(DateType).as(field.name))
            else if(field.`type`.startsWith("timestamp"))
                newDf = newDf.withColumn(field.name, df.col(field.name).cast(TimestampType).as(field.name))
            else
                throw new PipelineException("Internal error applying destination schema, dataType: " + field.`type` + ", is not supported")
        })

        newDf
    }

    private def reorderColumns(df: DataFrame): DataFrame = {
        if(config.destination.objectStore != null) {
            val destinationSchema = GlueUtil.getGlueSchema(config)
            val reorderedColumnNames = destinationSchema.fields.asScala.map(_.name)
            df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
        }
        else {
            // If there is no object store, therefore no Glue table, use the destination schema
            val reorderedColumnNames = config.destination.schemaProperties.fields.asScala.map(_.name)
            df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
        }
    }

    private def getSparkSourceSchema(config: DatasetConfig): StructType = {
        val schemaProperties = config.source.schemaProperties
        if(schemaProperties != null) {
            // Convert the config schema to spark DDL
            val schemaDDL = GlueUtil.glueSchemaToSparkDDL(Schema(schemaProperties.fields))
            StructType.fromDDL(schemaDDL)
        }
        else
            null
    }
}
