package net.idata.pipeline.model

import net.idata.pipeline.common.model.SchemaField

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

case class Data(
                    size: Long,
                    header: List[String],                   // Contains the header names, only used to validate the header of delimited data
                    headerWithSchema: List[SchemaField],    // Header SchemaFields, this can evolve as the data moves through the pipeline
                    rows: List[String],                     // List of rows, only for delimited data
                    rawData: String                         // Raw data, only for JSON and XML data
               )
