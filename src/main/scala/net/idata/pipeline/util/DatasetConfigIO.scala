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

import com.google.gson.Gson
import net.idata.pipeline.model.{DatasetConfig, PipelineEnvironment, PipelineException}


object DatasetConfigIO {
    def readAll(tableName: String): List[DatasetConfig] = {
        val datasetNames = NoSQLDbUtil.getItemsKeysByKeyName(tableName, "name")
        datasetNames.map(name => {
            read(PipelineEnvironment.values.datasetTableName, name)
        })
    }

    def read(tableName: String, datasetName: String): DatasetConfig = {
        val json = NoSQLDbUtil.getItemJSON(tableName, "name", datasetName, "value").orNull
        if(json != null) {
            val gson = new Gson
            val config = gson.fromJson(json, classOf[DatasetConfig])

            // If there are no destination schema properties, use the source schema as the destination schema
            if(config.destination.schemaProperties == null) {
                val destination = config.destination.copy(schemaProperties = config.source.schemaProperties)
                config.copy(destination = destination)
            }
            else
                config
        }
        else
            null
    }

    def write(datasetConfig: DatasetConfig): Unit = {
        val gson = new Gson
        val json = gson.toJson(datasetConfig)
        NoSQLDbUtil.putItemJSON(PipelineEnvironment.values.datasetTableName, "name", datasetConfig.name, "value", json)
    }

    def getSourceFileExtension(config: DatasetConfig): String = {
        val fileAttributes = config.source.fileAttributes
        if(fileAttributes.csvAttributes != null)
            "csv"
        else if(fileAttributes.jsonAttributes != null)
            "json"
        else if(fileAttributes.xmlAttributes != null)
            "xml"
        else if(fileAttributes.unstructuredAttributes != null)
            fileAttributes.unstructuredAttributes.fileExtension
        else
            throw new PipelineException("The dataset configuration fileAttributes are not configured properly")
    }
}
