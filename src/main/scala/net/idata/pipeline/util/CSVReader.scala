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

Author(s): Todd Fearn
*/

import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.JavaConverters._

class CSVReader {
    def readFile(url: String, header: Boolean, delimiter: String, columnList: List[String], columnFilter: List[String], removeHeader: Boolean = false): String = {
        // Determine the column #'s to read
        val columnNumbers = columnFilter.flatMap(filteredColumn => {
            columnList.zipWithIndex.flatMap { case (column, index) =>
                if (filteredColumn.equalsIgnoreCase(column))
                    Some(index)
                else
                    None
            }
        })

        // Read the file using Apache commons-csv
        val (bufferedReader, s3Object) = ObjectStoreUtil.getBufferedReader(ObjectStoreUtil.getBucket(url), ObjectStoreUtil.getKey(url))
        val parser = new CSVParser(bufferedReader, CSVFormat.RFC4180.builder().setDelimiter(delimiter).setIgnoreEmptyLines(true).build())

        // Get only the columns in the column filter
        val rows = parser.getRecords.asScala.map(record => {
            columnNumbers.map(colNumber => {
                record.get(colNumber)
            }).mkString(delimiter)
        }).toList

        if(header && removeHeader)
            rows.tail.mkString("\n")
        else
            rows.mkString("\n")
    }
}
