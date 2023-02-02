package net.idata.pipeline

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

Author(s): Todd Fearn
*/

import com.google.common.base.Throwables
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableScheduling


object PipelineServiceApplication {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

    def main(args: Array[String]) : Unit = {
        try {
            initializeApplication()
            SpringApplication.run(classOf[Application], args :_ *)
        } catch {
            case e: Exception =>
                logger.error("PipelineServiceApplication startup error: " + Throwables.getStackTraceAsString(e))
        }
    }

    def initializeApplication(): Unit = {
    }
}

@SpringBootApplication
@EnableScheduling
class Application {}
