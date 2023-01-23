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
