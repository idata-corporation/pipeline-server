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