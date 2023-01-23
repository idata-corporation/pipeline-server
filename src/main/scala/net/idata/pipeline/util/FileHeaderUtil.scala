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

import net.idata.pipeline.model.{DatasetConfig, PipelineException}

import java.util.regex.Pattern
import scala.collection.JavaConverters._

class FileHeaderUtil {
    def validateHeader(config: DatasetConfig, rows: String): Unit = {
        val fileExtension = DatasetConfigIO.getSourceFileExtension(config)

        // Must have a source schema and only valid for CSV files
        if(config.source.schemaProperties != null && fileExtension.compareToIgnoreCase("csv") == 0) {
            val header = rows.split("\\R", 2)
            val incomingColumns = header(0)
                .split(Pattern.quote(config.source.fileAttributes.csvAttributes.delimiter))
                .toList

            // The header must be in the exact order of the source schema if the source schema exists
            (incomingColumns, config.source.schemaProperties.fields.asScala).zipped.foreach { (column, schemaField) =>
                //println("Comparing column: " + column + ", to field: " + field.name)
                if(schemaField.name.compareToIgnoreCase(column) != 0)
                    throw new PipelineException("The incoming header on the data file does not match the destination schema for dataset: " + config.name + ", failed comparing column: " + column + " with source schema field: " + schemaField.name)
            }
        }
    }
}