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

import net.idata.pipeline.model.{DatasetConfig, JobContext, PipelineEnvironment, PipelineException}
import org.slf4j.{Logger, LoggerFactory}

import java.util.regex.Pattern
import scala.collection.JavaConverters._

class DataQuality(jobContext: JobContext) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DataQuality])
    private val config = jobContext.config
    private val statusUtil = jobContext.statusUtil

    def process(): JobContext = {
        statusUtil.overrideProcessName(this.getClass.getSimpleName)
        statusUtil.info("begin", "Process started")

        // Get the source files
        val files = DatasetMetadataUtil.getFiles(jobContext.metadata)

        // Validate the header of the file(s) for delimited files (if it has a header)
        if(config.dataQuality.validateFileHeader) {
            if(config.source.fileAttributes.csvAttributes != null && config.source.fileAttributes.csvAttributes.header) {
                statusUtil.info("processing", "Validating the incoming file header(s)")

                files.foreach(fileUrl => {
                    val header = ObjectStoreUtil.readBucketObjectFirstRow(ObjectStoreUtil.getBucket(fileUrl), ObjectStoreUtil.getKey(fileUrl))
                        .getOrElse("Could not read the data file from bucket: " + jobContext.bucket + ", key: " + jobContext.key)
                    validateHeader(jobContext.config, header)
                })
            }
        }

        // Validation schema?
        if(config.dataQuality.validationSchema != null) {
            val schemaFileUrl = {
                if(config.dataQuality.validationSchema.startsWith("s3://"))
                    config.dataQuality.validationSchema
                else
                    "s3://" + PipelineEnvironment.values.environment + "-config/validation-schema/" + config.dataQuality.validationSchema
            }

            files.foreach(fileUrl => {
                statusUtil.info("processing", "Validating the incoming data file: " + fileUrl + ", against the validation schema: " + schemaFileUrl)
                if(config.source.fileAttributes.jsonAttributes != null)
                    SchemaValidationUtil.validateJson(fileUrl, schemaFileUrl)
                else if(config.source.fileAttributes.xmlAttributes != null)
                    SchemaValidationUtil.validateXml(fileUrl, schemaFileUrl)
            })
        }

        // Dedup data?
        val jobContextDQ = {
            if(config.dataQuality.deduplicate) {
                statusUtil.info("processing", "Running deduplication")

                // Dedup each file and copy to a temp bucket
                val tempLocation = "s3://" + PipelineEnvironment.values.environment + "-temp/dataquality/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"
                var totalDeduped = 0
                files.foreach(fileUrl => {
                    val (deduped, count) = dedup(fileUrl, config)
                    totalDeduped = totalDeduped + count

                    val tempFilename = jobContext.config.name + "." +  GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
                    ObjectStoreUtil.writeBucketObject(
                        ObjectStoreUtil.getBucket(tempLocation + tempFilename),
                        ObjectStoreUtil.getKey(tempLocation + tempFilename),
                        deduped)
                })
                statusUtil.info("processing", totalDeduped.toString + " rows were duplicates and removed")

                val metadata = jobContext.metadata.copy(transformedPath = tempLocation)
                jobContext.copy(metadata = metadata)
            }
            else
                jobContext
        }

        statusUtil.info("end", "Process completed successfully")
        jobContextDQ
    }

    private def validateHeader(config: DatasetConfig, rows: String): Unit = {
        val header = rows.split("\\R", 2)
        val incomingColumns = header(0)
            .split(Pattern.quote(config.source.fileAttributes.csvAttributes.delimiter))
            .toList

        // The header must be in the exact order of the source schema if the source schema exists
        (incomingColumns, config.source.schemaProperties.fields.asScala).zipped.foreach { (column, schemaField) =>
            //logger.info("Comparing header column: " + column + ", to field: " + field.name)
            if(schemaField.name.compareToIgnoreCase(column) != 0)
                throw new PipelineException("The incoming header on the data file does not match the destination schema for dataset: " + config.name + ", failed comparing column: " + column + " with source schema field: " + schemaField.name)
        }
    }

    private def dedup(fileUrl: String, config: DatasetConfig): (String, Int) = {
        logger.info("Performing deduplication on file: " + fileUrl)

        val file = new CSVReader().readFile(fileUrl,
            config.source.fileAttributes.csvAttributes.header,
            config.source.fileAttributes.csvAttributes.delimiter,
            config.source.schemaProperties.fields.asScala.map(_.name).toList,
            config.source.schemaProperties.fields.asScala.map(_.name).toList)

        val lines = file.split("\n").toList
        val distinct = lines.distinct
        val deduped = lines.size - distinct.size
        if(deduped > 0)
            logger.info(deduped.toString + " rows were deduplicated from source file: " + fileUrl)

        (distinct.mkString("\n"), deduped)
    }
}
