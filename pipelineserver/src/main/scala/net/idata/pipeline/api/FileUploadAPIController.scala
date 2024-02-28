package net.idata.pipeline.api

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

import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.common.base.Throwables
import io.deephaven.csv.CsvSpecs
import io.deephaven.csv.parsers.DataType._
import io.deephaven.csv.reading.CsvReader
import io.deephaven.csv.sinks.SinkFactory
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{DatasetConfigIO, ObjectStoreUtil}
import net.idata.pipeline.util.APIKeyValidator
import org.apache.commons.compress.utils.FileNameUtils
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation._
import org.springframework.web.multipart.MultipartFile

import java.io.ByteArrayInputStream
import java.text.SimpleDateFormat
import java.util.Date

@RestController
@CrossOrigin(origins = Array("*"), methods = Array(RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS))
class FileUploadAPIController {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DatasetAPIController])

    @PostMapping(path = Array("/dataset/upload"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def uploadRawFile(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                      @RequestPart("file") multipartFile: MultipartFile,
                      @RequestParam("dataset") dataset: String,
                      @RequestParam(required = false) publishertoken: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint POST /dataset/upload called for dataset: " + dataset + ", filename: " + multipartFile.getOriginalFilename + ", publishertoken: " + publishertoken)
            APIKeyValidator.validate(apiKey)

            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, dataset)
            if(config == null)
                throw new PipelineException("Dataset: " + dataset + " is not configured in the NoSQL database")

            // Determine the raw filename
            val rawFilename = {
                val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH-mm-ss-SSS")
                val date = dateFormat.format(new Date())

                val fileExtension = {
                    if(isCompressed(multipartFile.getOriginalFilename))
                        FileNameUtils.getExtension(multipartFile.getOriginalFilename)
                    else
                        DatasetConfigIO.getSourceFileExtension(config)
                }
                if(publishertoken != null)
                    config.name + "." + publishertoken + "." + date + "." + System.currentTimeMillis().toString + ".dataset." + fileExtension
                else
                    config.name + "." + date + "." + System.currentTimeMillis().toString + ".dataset." + fileExtension
            }

            // Write the data to the raw bucket
            val byteArray = multipartFile.getBytes
            val contentLength = byteArray.length
            val metadata = new ObjectMetadata()
            metadata.setContentLength(contentLength)
            val fileStream = new ByteArrayInputStream(byteArray)
            val path = "s3://" + PipelineEnvironment.values.environment + "-raw/temp/" + config.name + "/" + rawFilename
            ObjectStoreUtil.writeBucketObjectFromStream(ObjectStoreUtil.getBucket(path), ObjectStoreUtil.getKey(path), fileStream, metadata)

            new ResponseEntity[String](HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @PostMapping(path = Array("/dataset/generate"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def datasetGenerate(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                        @RequestPart("file") multipartFile: MultipartFile,
                        @RequestParam("dataset") dataset: String,
                        @RequestParam(required = false) delimiter: String,
                        @RequestParam(required = false) header: Boolean): ResponseEntity[String] = {
        try {
            logger.info("API endpoint POST /dataset/generate called for dataset: " + dataset + ", filename: " + multipartFile.getOriginalFilename)
            APIKeyValidator.validate(apiKey)

            val json = {
                if (multipartFile.getOriginalFilename.endsWith(".json") || multipartFile.getOriginalFilename.endsWith(".xml"))
                    null
                else
                    generateCsvDataset(multipartFile, dataset, delimiter, header)
            }
            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    private def isCompressed(fileUrl: String): Boolean = {
        fileUrl.toLowerCase.endsWith(".zip") || fileUrl.toLowerCase.endsWith(".gz") || fileUrl.toLowerCase.endsWith(".tar") || fileUrl.toLowerCase.endsWith(".jar")
    }

    private def generateCsvDataset(multipartFile: MultipartFile, dataset: String, delimiter: String, header: Boolean): String = {
        // Read the data file using deephaven-csv to determine the field types
        val myDelimiter = {
            if(delimiter == null)
                ","
            else
                delimiter
        }
        val byteArray = multipartFile.getBytes
        val fileStream = new ByteArrayInputStream(byteArray)
        val specs = CsvSpecs.builder().delimiter(myDelimiter(0)).build()
        val result = CsvReader.read(specs, fileStream, SinkFactory.arrays())

        // Build the dataset JSON
        val json = new StringBuilder()
        json.append("{ \"name\": \"" + dataset + "\",")
        json.append(" \"source\": {")
        json.append(" \"fileAttributes\": {")
        json.append(" \"csvAttributes\": {")
        json.append(" \"delimiter\": \"" + myDelimiter + "\",")
        json.append(" \"encoding\": \"UTF-8\",")
        if(header)
            json.append(" \"header\": true")
        else
            json.append(" \"header\": false")
        json.append(" } },")

        // fields
        json.append(" \"schemaProperties\": {")
        json.append(" \"fields\": [")
        result.columns().foreach(col => {
            val fieldType = {
                col.dataType() match {
                    case (SHORT | INT) => "int"
                    case LONG => "bigint"
                    case FLOAT => "float"
                    case DOUBLE => "double"
                    case DATETIME_AS_LONG => "bigint"
                    case CHAR => "char"
                    case STRING => "string"
                    case BOOLEAN_AS_BYTE => "string"
                    case BYTE => "string"
                    case TIMESTAMP_AS_LONG => "bigint"
                    case _ => "string"
                }
            }
            json.append(" { \"name\": \"" + col.name() + "\",")
            json.append(" \"type\": \"" +  fieldType + "\" },")
        })
        json.deleteCharAt(json.length-1)
        json.append(" ] } },")

        // destination
        json.append(" \"destination\": {")
        json.append(" \"database\": {")
        json.append(" \"dbName\": \"DATABASE_NAME\",")
        json.append(" \"schema\": \"SCHEMA_NAME\",")
        json.append(" \"table\": \"TABLE_NAME\",")

        json.append(" \"snowflake\": {")
        json.append(" \"_comment\": \"remove snowflake section if not used\",")
        json.append(" \"warehouse\": \"WAREHOUSE_NAME\",")
        json.append(" \"keyFields\": [\"KEY_FIELD1\", \"KEY_FIELD2\"]")
        json.append(" }, ")

        json.append(" \"redshift\": {")
        json.append(" \"_comment\": \"remove redshift section if not used\",")
        json.append(" \"keyFields\": [\"KEY_FIELD1\", \"KEY_FIELD2\"]")
        json.append(" } ")

        json.append( "} } }")

        json.toString
    }
}