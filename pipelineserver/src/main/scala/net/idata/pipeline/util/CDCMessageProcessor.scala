package net.idata.pipeline.util

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
import net.idata.pipeline.common.model.{DatasetConfig, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.DatasetConfigIO
import net.idata.pipeline.model.DebeziumMessage
import org.slf4j.{Logger, LoggerFactory}

class CDCMessageProcessor {
    private val logger: Logger = LoggerFactory.getLogger(classOf[CDCMessageProcessor])

    def process(messages: List[DebeziumMessage]): Unit = {
        val configs = messages.map(message => {
            val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, message.datasetName)
            if (config == null)
                throw new PipelineException("CDC message error, dataset configuration: " + message.datasetName + " was not found in the NoSQL table: " + PipelineEnvironment.values.datasetTableName)
            config
        }).distinct

        val groupedByDestination = messages.map(message => {
            val config = configs.find(_.name.compareTo(message.datasetName) == 0).orNull
            if (config.destination.objectStore != null)
                ("objectStore", config, message)
            else if (config.destination.database != null && config.destination.database.snowflake != null)
                ("snowflake", config, message)
            else if (config.destination.database != null && config.destination.database.redshift != null)
                ("redshift", config, message)
            else
                throw new PipelineException("The proper destination is not set for the dataset " + config.name)
        }).groupBy(_._1)

        groupedByDestination.foreach { case (_, groupedMessages) =>
            processGroup(groupedMessages)
        }
    }

    private def processGroup(groupOfMessages: List[(String, DatasetConfig, DebeziumMessage)]): Unit = {
        try {
            val destination = groupOfMessages.head._1
            destination match {
                case "objectStore" =>
                    new ObjectStoreCDCUtil().process(groupOfMessages)
                case "snowflake" =>
                    // TODO
                case "redshift" =>
                    new RedshiftCDCUtil().process(groupOfMessages)
            }
        }
        catch {
            case e: Exception =>
                logger.info("Error writing CDC message to store: " + Throwables.getStackTraceAsString(e))
            // TODO - put this message in a queue or something to save it
        }
    }
}