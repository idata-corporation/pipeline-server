package net.idata.pipeline.util

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
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
        else
            throw new PipelineException("The dataset configuration fileAttributes are not configured properly")
    }
}
