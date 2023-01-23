package net.idata.pipeline.api

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

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.util.{APIKeyValidator, DatasetStatusUtil}
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