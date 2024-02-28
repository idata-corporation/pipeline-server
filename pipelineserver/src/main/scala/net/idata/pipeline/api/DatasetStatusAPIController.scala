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

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.common.util.DatasetStatusUtil
import net.idata.pipeline.util.APIKeyValidator
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation._

@RestController
@CrossOrigin(origins = Array("*"),  methods = Array(RequestMethod.GET, RequestMethod.OPTIONS))
class
DatasetStatusAPIController {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DatasetStatusAPIController])

    @GetMapping(path = Array("/dataset/status"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getDatasetStatus(@RequestHeader(name = "x-api-key", required = false) apiKey: String,
                         @RequestParam(required = false) pipelinetoken: String,
                         @RequestParam(required=false) datasetname: String,
                         @RequestParam(required = false) page: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /dataset/status called with pipelinetoken: " + pipelinetoken + ", datasetname: " + datasetname + ", page: " + page)
            APIKeyValidator.validate(apiKey)

            val data = {
                if(pipelinetoken == null) {
                    val pageNbr = {
                        if(page == null)
                            1
                        else
                            page.toInt
                    }
                    DatasetStatusUtil.getDatasetStatusSummary(datasetname, pageNbr)
                }
                else
                    DatasetStatusUtil.getDatasetStatus(pipelinetoken)
            }
            val gson = new Gson
            new ResponseEntity[String](gson.toJson(data), HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }
}