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

import net.idata.pipeline.model.{DatasetConfig, PipelineException}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RowUtil {
    def getRowAsMap(row: String, config: DatasetConfig): mutable.ListMap[String, Any] = {
        val columnsWithIndex = config.source.schemaProperties.fields.asScala.zipWithIndex.toList

        // Map the row data by field type
        val columnMap = mutable.ListMap[String, Any]()
        config.source.schemaProperties.fields.asScala.map(column => {

            // Find the column value
            val (schemaField, columnNumber) = columnsWithIndex.find { case (columnWithIndex, columnNumber) =>
                columnWithIndex.name.compareToIgnoreCase(column.name) == 0
            }.getOrElse(throw new PipelineException("Internal error, could not find the field name: " + column.name))
            val columns = row.split(config.source.fileAttributes.csvAttributes.delimiter).toList
            val columnValue = columns(columnNumber)

            // Add to the map
            if(columnValue == null || columnValue.isEmpty)
                columnMap.put(column.name, columnValue)
            else {
                if(column.`type`.startsWith("boolean"))
                    columnMap.put(column.name, columnValue.toBoolean)
                else if(column.`type`.startsWith("int"))
                    columnMap.put(column.name, columnValue.toInt)
                else if(column.`type`.startsWith("tinyint"))
                    columnMap.put(column.name, columnValue.toShort)
                else if(column.`type`.startsWith("smallint"))
                    columnMap.put(column.name, columnValue.toShort)
                else if(column.`type`.startsWith("bigint"))
                    columnMap.put(column.name, columnValue.toLong)
                else if(column.`type`.startsWith("float"))
                    columnMap.put(column.name, columnValue.toFloat)
                else if(column.`type`.startsWith("double"))
                    columnMap.put(column.name, columnValue.toDouble)
                else if(column.`type`.startsWith("decimal"))
                    columnMap.put(column.name, columnValue.toDouble)
                else if(column.`type`.startsWith("string"))
                    columnMap.put(column.name, columnValue)
                else if(column.`type`.startsWith("varchar"))
                    columnMap.put(column.name, columnValue)
                else if(column.`type`.startsWith("char"))
                    columnMap.put(column.name, columnValue)
                else if(column.`type`.startsWith("date"))
                    columnMap.put(column.name, columnValue)
                else if(column.`type`.startsWith("timestamp"))
                    columnMap.put(column.name, columnValue)
                else
                    throw new PipelineException("Internal error applying destination schema, dataType: " + column.`type` + ", is not supported")
            }
        })
        columnMap
    }
}