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

import net.idata.pipeline.model.{Data, DatasetConfig, DatasetMetadata, PipelineException}

import java.util.regex.Pattern
import scala.collection.JavaConverters._

object DataUtil {
    def read(bucket: String, key: String, config: DatasetConfig, metadata: DatasetMetadata): Data = {
        val files = DatasetMetadataUtil.getFiles(metadata)
        val size = getSize(bucket, key, metadata)

        if(config.source.fileAttributes.csvAttributes != null) {
            var header: List[String] = null
            val data = files.zipWithIndex.flatMap { case (fileUrl, index) =>
                val rows = {
                    if (index == 0) {
                        val data = new CSVReader().readFile(fileUrl,
                            config.source.fileAttributes.csvAttributes.header,
                            config.source.fileAttributes.csvAttributes.delimiter,
                            config.source.schemaProperties.fields.asScala.map(_.name).toList,
                            config.source.schemaProperties.fields.asScala.map(_.name).toList)
                            .split("\n")
                            .toList
                        if(config.source.fileAttributes.csvAttributes.header) {
                            header = data.head.split(Pattern.quote(config.source.fileAttributes.csvAttributes.delimiter)).toList
                            data.tail
                        }
                        else
                            data
                    }
                    else {
                        new CSVReader().readFile(fileUrl,
                            config.source.fileAttributes.csvAttributes.header,
                            config.source.fileAttributes.csvAttributes.delimiter,
                            config.source.schemaProperties.fields.asScala.map(_.name).toList,
                            config.source.schemaProperties.fields.asScala.map(_.name).toList,
                            removeHeader = true)
                            .split("\n")
                            .toList
                    }
                }
                rows
            }
            Data(size, header, data, null)
        }
        else if(config.source.fileAttributes.jsonAttributes != null || config.source.fileAttributes.xmlAttributes != null) {
            val fileUrl = files.head
            val rawData = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(fileUrl), ObjectStoreUtil.getKey(fileUrl))
                .getOrElse(throw new PipelineException("Error reading source file: " + fileUrl))
            Data(size, null, null, rawData)
        }
        else
            null
    }

    private def getSize(bucket: String, key: String, metadata: DatasetMetadata): Long = {
        // Get the file size
        val objectMetadata = ObjectStoreUtil.getObjectMetatadata(bucket, key)
        val objectSize = {
            // Bulk file ingestion?
            if(metadata.dataFilePath != null) {
                val summaries = ObjectStoreUtil.listSummaries(ObjectStoreUtil.getBucket(metadata.dataFilePath),
                    ObjectStoreUtil.getKey(metadata.dataFilePath))
                summaries.map(_.getSize).sum
            }
            else
                objectMetadata.getContentLength
        }

        objectSize
    }
}
