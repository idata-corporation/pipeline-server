package net.idata.pipeline.util

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

import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.ObjectStoreUtil
import net.idata.pipeline.model.JobContext

import java.text.SimpleDateFormat
import java.util.Date
import javax.script.ScriptEngineManager
import scala.collection.JavaConverters._
import scala.collection.mutable

class Transformation(jobContext: JobContext) {
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): JobContext = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        statusUtil.info("begin", "Process started")

        val jobContextDD = {
            if(config.transformation.deduplicate)
                deduplicate(jobContext)
            else
                jobContext
        }

        val jobContextRF = {
            if(config.transformation.rowFunctions != null)
                runRowFunctions(jobContextDD)
            else
                jobContextDD
        }

        statusUtil.info("end", "Process completed successfully")
        jobContextRF
    }

    private def deduplicate(jobContext: JobContext): JobContext = {
        statusUtil.info("processing", "Running deduplication")

        val distinct = jobContext.data.rows.distinct
        val deduped = jobContext.data.rows.size - distinct.size
        if(deduped > 0) {
            statusUtil.info("processing", deduped.toString + " rows were duplicates and removed")
            val newData = jobContext.data.copy(rows = distinct)
            jobContext.copy(data = newData)
        }
        else
            jobContext
    }

    private def runRowFunctions(jobContextRF: JobContext): JobContext = {
        // Find the javaScript" function for the data
        val scriptFunction = config.transformation.rowFunctions.asScala.flatMap(rowFunction => {
            if(rowFunction.function.compareToIgnoreCase("javascript") == 0) {
                Some(rowFunction)
            } else
                None
        }).toList
            .head

        if(scriptFunction != null) {
            if(scriptFunction.parameters == null || scriptFunction.parameters.isEmpty)
                throw new PipelineException("Javascript row function '" + scriptFunction.function + "' does not contain any parameters")

            // Read the javascript from the path in parameter 0
            val filePath = scriptFunction.parameters.get(0)
            val javascript = {
                val url = {
                    if(filePath.startsWith("s3"))
                        filePath
                    else {
                        // Build the path assuming the filePath is just the filename
                        "s3://" + PipelineEnvironment.values.environment + "-config/javascript/" + filePath
                    }
                }
                statusUtil.info("processing", "Running row function: javascript, using script: " + url)

                ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(url), ObjectStoreUtil.getKey(url)).getOrElse(
                    throw new PipelineException("Javascript file not found using the first parameter of the row function: " + filePath))
            }

            // Cycle through the rows and run the javascript function
            var removed: Long = 0
            val transformed = jobContextRF.data.rows.flatMap(row => {
                val columnMap = RowUtil.getRowAsMap(row, config)
                val changedValues = runScript(columnMap, javascript)
                if(changedValues != null) {
                    val row = config.destination.schemaProperties.fields.asScala.map(field => {
                        val value = changedValues.get(field.name)
                        if(value == null)
                            columnMap.getOrElse(field.name, "")
                        else
                            value.toString
                    }).toList
                        .mkString(config.source.fileAttributes.csvAttributes.delimiter)
                    Some(row)
                }
                else {
                    removed = removed + 1
                    None
                }
            })

            if(removed > 0)
                statusUtil.info("processing", removed.toString + " rows were removed during the javascript transformation")

            val headerWithSchema = config.destination.schemaProperties.fields.asScala.toList
            val newData = jobContextRF.data.copy(headerWithSchema = headerWithSchema, rows = transformed)
            jobContextRF.copy(data = newData)
        }
        else
            null
    }

    private def runScript(columnMap: mutable.ListMap[String, Any], script: String): java.util.HashMap[String, Any] = {
        val engine = new ScriptEngineManager().getEngineByName("JavaScript")
        val bindings = engine.createBindings()

        // Add all of the column key/values as parameters
        columnMap.foreach { case (key, value) => bindings.put(key, value) }

        // Add the _pipelinetimestamp as the last parameter
        val formatter= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z")
        val pipelineTimestamp = formatter.format(new Date(System.currentTimeMillis()))
        bindings.put("_pipelinetimestamp", pipelineTimestamp)

        engine.eval(script, bindings).asInstanceOf[java.util.HashMap[String, Any]]
    }
}