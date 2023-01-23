package net.idata.pipeline

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
