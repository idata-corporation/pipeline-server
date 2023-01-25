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

import net.idata.pipeline.model.{ColumnRule, JobContext, PipelineException}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class DataRulesUtil(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DataRulesUtil])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def runColumnRules(): Unit = {
        val files = DatasetMetadataUtil.getFiles(jobContext.metadata)

        val results = files.flatMap(fileUrl => {
            statusUtil.info("processing", "Performing data quality rules on file: " + fileUrl)

            // Read the file
            val rows = new CSVReader().readFile(fileUrl,
                config.source.fileAttributes.csvAttributes.header,
                config.source.fileAttributes.csvAttributes.delimiter,
                config.source.schemaProperties.fields.asScala.map(_.name).toList,
                config.source.schemaProperties.fields.asScala.map(_.name).toList,
                removeHeader = true)
                .split("\n")

            rows.zipWithIndex.flatMap { case (row, rowNumber) =>
                config.dataQuality.columnRules.asScala.flatMap(rule => {
                    val (schemaField, columnNumber) = config.source.schemaProperties.fields.asScala.zipWithIndex.find { case (field, fieldNumber) =>
                        field.name.compareToIgnoreCase(rule.columnName) == 0
                    }.getOrElse(throw new PipelineException("Column rule field: " + rule.columnName + " was not found in the source 'schemaProperties' for this dataset"))

                    val columns = row.split(config.source.fileAttributes.csvAttributes.delimiter).toList
                    val columnValue = columns(columnNumber)

                    rule.function match {
                        case "regex" =>
                            if (!regex(rule, columnValue))
                                Some((rule.onFailureIsError, "Data quality regular expression failure, file: " + fileUrl + " row: " + (rowNumber + 1).toString + ", column: " + rule.columnName.toLowerCase + ", rule: " + rule.function + "=" + rule.parameter))
                            else
                                None

                        case _ => throw new PipelineException("Data quality rule: " + rule.function + " for column: " + rule.columnName.toLowerCase + " is not defined in the Data Quality Engine")
                    }
                }).toList
            }
        })

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
                logger.error(message)
                if(errorCount > 100)
                    throw new PipelineException("Aborting processing, more than 100 data quality column rule errors.  Check the log for details")
            }
            else {
                warningCount = warningCount + 1
                logger.warn(message)
            }
        }
        if(errorCount > 0)
            throw new PipelineException("Aborting processing this dataset, " + errorCount.toString + " error(s) were found while performing data quality rules.  Check the log for details")
        if(warningCount > 0)
            statusUtil.warn("processing", warningCount.toString + " warning(s) occured while processing this dataset.  Check the log for details")
    }
}