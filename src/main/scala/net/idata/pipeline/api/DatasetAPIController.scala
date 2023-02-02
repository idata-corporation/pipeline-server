package net.idata.pipeline.api

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

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.model.{DatasetConfig, PipelineEnvironment, PipelineException}
import net.idata.pipeline.util._
import net.idata.pipeline.util.aws.GlueUtil
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation._

import scala.collection.JavaConverters._

@RestController
@CrossOrigin(origins = Array("*"), methods = Array(RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.OPTIONS))
class DatasetAPIController {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DatasetAPIController])

    @GetMapping(path = Array("/dataset"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getDataset(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                         @RequestParam dataset: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /dataset called with dataset: " + dataset)
            APIKeyValidator.validate(apiKey)

            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, dataset)
            if(config == null)
                throw new PipelineException("Dataset: " + dataset + " is not configured in the NoSQL database")
            val gson = new Gson
            val json = gson.toJson(config)
            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @GetMapping(path = Array("/datasets"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getDatasets(@RequestHeader(name = "x-api-key", required = false) apiKey: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /datasets called")
            APIKeyValidator.validate(apiKey)

            val datasetNames = NoSQLDbUtil.getItemsKeysByKeyName(PipelineEnvironment.values.datasetTableName, "name")
            val datasetConfigs = datasetNames.map(name => {
                DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, name)
            }).asJava

            val gson = new Gson
            val json = gson.toJson(datasetConfigs)
            new ResponseEntity[String](json, HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @PostMapping(path = Array("/dataset"), consumes = Array(MediaType.APPLICATION_JSON_VALUE), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def putDataset(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                         @RequestBody config: DatasetConfig): ResponseEntity[String] = {
        try {
            logger.info("API endpoint POST /dataset with dataset name: " + config.name)
            APIKeyValidator.validate(apiKey)

            DatasetValidatorUtil.validate(config)
            val configLowerCase = DatasetValidatorUtil.lowercaseConfig(config)

            // Write to DynamoDb dataset table
            DatasetConfigIO.write(configLowerCase)

            // If the destination is object store, create a Glue table if not manually managing Glue for this dataset?
            if(config.destination.objectStore != null) {
                if(config.destination.objectStore.manageGlueTableManually)
                    logger.warn("A Glue table will not be created for this dataset because the 'destination.objectStore.manageGlueTableManually' field is set to 'true'.  The dataset Glue table must be created manually")
                else {
                    // Read back the config so the destination.schemaProperties are set if they did not exist
                    val newConfig = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, config.name)

                    // Create a Glue table for source CSV data
                    if(config.source.fileAttributes.csvAttributes != null)
                        DatasetObjectStoreUtil.createTable(newConfig)
                }
            }

            new ResponseEntity[String](HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }

    @DeleteMapping(path = Array("/dataset"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def deleteDataset(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                            @RequestParam dataset: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint DELETE /dataset with dataset name: " + dataset)
            APIKeyValidator.validate(apiKey)

            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, dataset)
            if(config == null)
                throw new PipelineException("Dataset: " + dataset + " is not configured in the NoSQL database")

            // Delete the json configuration
            NoSQLDbUtil.deleteItemJSON(PipelineEnvironment.values.datasetTableName, "name", dataset)

            // Delete the Glue table
            if(config.destination.objectStore != null)
                GlueUtil.dropTable(config.destination.schemaProperties.dbName, config.name)

            new ResponseEntity[String](HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }
}
