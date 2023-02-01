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

import net.idata.pipeline.model.{JobContext, PipelineEnvironment, PipelineException}

import javax.script.ScriptEngineManager
import scala.collection.JavaConverters._
import scala.collection.mutable

class Transformation(jobContext: JobContext) {
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): JobContext = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        statusUtil.info("begin", "Process started")

        val jobContextRF = {
            if(config.transformation.rowFunctions != null)
                runRowFunctions(jobContext)
            else
                jobContext
        }

        val jobContextCF = {
            jobContextRF
        }

        statusUtil.info("end", "Process completed successfully")
        jobContextCF
    }

    private def runRowFunctions(jobContext: JobContext): JobContext = {
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
            val transformed = jobContext.data.rows.map(row => {
                val columnMap = RowUtil.getRowAsMap(row, config)
                val changedValues = runScript(columnMap, javascript)

                config.destination.schemaProperties.fields.asScala.map(field => {
                    val value = changedValues.get(field.name)
                    if(value == null)
                        columnMap.getOrElse(field.name, null)
                    else
                        value.toString
                }).toList
                    .mkString(config.source.fileAttributes.csvAttributes.delimiter)
            })

            val newData = jobContext.data.copy(rows = transformed)
            jobContext.copy(data = newData)
        }
        else
            null
    }

    private def runScript(columnMap: mutable.ListMap[String, Any], script: String): java.util.HashMap[String, Any] = {
        val engine = new ScriptEngineManager().getEngineByName("JavaScript")
        val bindings = engine.createBindings()
        columnMap.foreach { case (key, value) => bindings.put(key, value) }

        engine.eval(script, bindings).asInstanceOf[java.util.HashMap[String, Any]]
    }
}