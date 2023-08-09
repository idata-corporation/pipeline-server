package net.idata.pipeline.util

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
*/

import net.idata.pipeline.model._

import javax.script.ScriptEngineManager
import scala.collection.JavaConverters._
import scala.collection.mutable

class DataQuality(jobContext: JobContext) {
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): Unit = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        statusUtil.info("begin", "Process started")

        // Validate the header of the file(s) for delimited files (if it has a header)
        if(config.dataQuality.validateFileHeader) {
            if(config.source.fileAttributes.csvAttributes != null && config.source.fileAttributes.csvAttributes.header) {
                statusUtil.info("processing", "Validating the incoming file header(s)")

                validateHeader(jobContext.data.header, jobContext.config)
            }
        }

        // Validation schema?
        if(config.dataQuality.validationSchema != null) {
            val schemaFileUrl = {
                if(config.dataQuality.validationSchema.startsWith("s3://"))
                    config.dataQuality.validationSchema
                else
                    "s3://" + PipelineEnvironment.values.environment + "-config/validation-schema/" + config.dataQuality.validationSchema
            }

            statusUtil.info("processing", "Validating the incoming data for dataset: " + config.name + ", against the validation schema: " + schemaFileUrl)
            if(config.source.fileAttributes.jsonAttributes != null)
                SchemaValidationUtil.validateJson(jobContext.data.rawData, schemaFileUrl)
            else if(config.source.fileAttributes.xmlAttributes != null)
                SchemaValidationUtil.validateXml(jobContext.data.rawData, schemaFileUrl)
        }

        // Row rules?
        if(config.dataQuality.rowRules != null)
            runRowRules(jobContext.data.rows)

        // Column rules?
        if(config.dataQuality.columnRules != null)
            runColumnRules(jobContext.data.rows)

        statusUtil.info("end", "Process completed successfully")
    }

    private def validateHeader(header: List[String], config: DatasetConfig): Unit = {
        // The header must be in the exact order of the source schema if the source schema exists
        (header, config.source.schemaProperties.fields.asScala).zipped.foreach { (column, schemaField) =>
            //logger.info("Comparing header column: " + column + ", to field: " + field.name)
            if(schemaField.name.compareToIgnoreCase(column) != 0)
                throw new PipelineException("The incoming header on the data file does not match the destination schema for dataset: " + config.name + ", failed comparing column: " + column + " with source schema field: " + schemaField.name)
        }
    }

    private def runRowRules(rows: List[String]): Unit = {
        // Gather all of the "javaScript" rules
        val scriptRules = config.dataQuality.rowRules.asScala.flatMap(rowRule => {
            if(rowRule.function.compareToIgnoreCase("javascript") == 0)
                Some(rowRule)
            else
                None
        }).toList

        if(scriptRules != null && scriptRules.nonEmpty) {
            val results = scriptRules.flatMap(rule => {
                if (rule.parameters == null || rule.parameters.isEmpty)
                    throw new PipelineException("Javascript row rule '" + rule.function + "' does not contain any parameters")

                statusUtil.info("processing", "Running data quality row rule: javascript, using script: " + rule.parameters.get(0))

                // Read the javascript file from the path in parameter 0
                val filePath = rule.parameters.get(0)
                val javascript = {
                    val url = {
                        if (filePath.startsWith("s3"))
                            filePath
                        else {
                            // Build the path assuming the filePath is just the filename
                            "s3://" + PipelineEnvironment.values.environment + "-config/javascript/" + filePath
                        }
                    }
                    ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(url), ObjectStoreUtil.getKey(url)).getOrElse(
                        throw new PipelineException("Javascript file not found using the first parameter of the row rule: " + filePath))
                }

                // Cycle through the rows and run the row functions
                rows.zipWithIndex.flatMap { case (row, rowNumber) =>
                    val columnMap = RowUtil.getRowAsMap(row, config)
                    val description = runScript(columnMap, javascript)

                    if (description != null)
                        Some(rule.onFailureIsError, "Data quality failure, row: " + (rowNumber + 2).toString + ", description: " + description)
                    else
                        None
                }
            })

            dumpResults(results)
        }
    }

    private def runColumnRules(rows: List[String]): Unit = {
        statusUtil.info("processing", "Performing data quality column rules")

        val results = rows.zipWithIndex.flatMap { case (row, rowNumber) =>
            config.dataQuality.columnRules.asScala.flatMap(rule => {
                val (schemaField, columnNumber) = config.source.schemaProperties.fields.asScala.zipWithIndex.find { case (field, fieldNumber) =>
                    field.name.compareToIgnoreCase(rule.columnName) == 0
                }.getOrElse(throw new PipelineException("Column rule field: " + rule.columnName + " was not found in the source 'schemaProperties' for this dataset"))

                val columns = row.split(config.source.fileAttributes.csvAttributes.delimiter).toList
                val columnValue = columns(columnNumber)

                rule.function match {
                    case "regex" =>
                        if (!regex(rule, columnValue))
                            Some((rule.onFailureIsError, "Data quality regular expression failure on row: " + (rowNumber + 2).toString + ", column: " + rule.columnName.toLowerCase + ", rule: " + rule.function + "=" + rule.parameter))
                        else
                            None

                    case _ => throw new PipelineException("Data quality rule: " + rule.function + " for column: " + rule.columnName.toLowerCase + " is not defined in the Data Quality Engine")
                }
            }).toList
        }

        dumpResults(results)
    }

    private def regex(rule: ColumnRule, value: String): Boolean = {
        value.matches(rule.parameter)
    }

    private def dumpResults(results: List[(Boolean, String)]): Unit = {
        var errorCount: Int = 0
        var warningCount: Int = 0

        results.foreach { case (onFailureIsError, message) =>
            if (onFailureIsError) {
                errorCount = errorCount + 1
                statusUtil.error("processing", message)
                if(errorCount > 100)
                    throw new PipelineException("Aborting processing, more than 100 data quality column rule errors")
            }
            else {
                warningCount = warningCount + 1
                statusUtil.warn("processing", message)
            }
        }
        if(errorCount > 0)
            throw new PipelineException("Aborting processing this dataset, " + errorCount.toString + " error(s) were found while performing data quality rules")
        if(warningCount > 0)
            statusUtil.warn("processing", warningCount.toString + " warning(s) occured while processing this dataset")
    }

    private def runScript(columnDataMap: mutable.ListMap[String, Any], script: String): String = {
        val engine = new ScriptEngineManager().getEngineByName("JavaScript")
        val bindings = engine.createBindings()
        columnDataMap.foreach { case (key, value) => bindings.put(key, value) }

        engine.eval(script, bindings).asInstanceOf[String]
    }
}
