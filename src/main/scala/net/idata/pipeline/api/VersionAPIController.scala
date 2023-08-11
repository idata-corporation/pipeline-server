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
*/

import com.google.common.base.Throwables
import com.google.gson.Gson
import net.idata.pipeline.build.sbt.BuildInfo
import net.idata.pipeline.util.APIKeyValidator
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.{HttpStatus, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation._

import scala.collection.JavaConverters._

@RestController
@CrossOrigin(origins = Array("*"),  methods = Array(RequestMethod.GET, RequestMethod.OPTIONS))
class VersionAPIController {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DatasetStatusAPIController])

    @GetMapping(path = Array("/version"), produces = Array(MediaType.APPLICATION_JSON_VALUE))
    def getVersion(@RequestHeader(name = "x-api-key", required = false) apiKey: String): ResponseEntity[String] = {
        try {
            logger.info("API endpoint GET /version called")
            APIKeyValidator.validate(apiKey)
            val map = Map("version" -> BuildInfo.version).asJava
            val gson = new Gson
            new ResponseEntity[String](gson.toJson(map), HttpStatus.OK)
        }
        catch {
            case e: Exception =>
                logger.error("Error: " + Throwables.getStackTraceAsString(e))
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body[String](Throwables.getStackTraceAsString(e))
        }
    }
}