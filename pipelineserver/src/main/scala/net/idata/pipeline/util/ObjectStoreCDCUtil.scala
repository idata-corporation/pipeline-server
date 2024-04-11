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


import net.idata.pipeline.common.model.{DatasetConfig, PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, ObjectStoreSQLUtil}
import net.idata.pipeline.model.CDCMessage
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant

class ObjectStoreCDCUtil {
    private val logger: Logger = LoggerFactory.getLogger(classOf[ObjectStoreCDCUtil])

    def process(groupOfMessages: List[(String, DatasetConfig, CDCMessage)]): Unit = {
        val config = groupOfMessages.head._2
        val containsDeletes = groupOfMessages.find(_._3.isDelete == true)

        if(containsDeletes.isDefined && !config.destination.objectStore.useIceberg)
            throw new PipelineException("CDC 'deletes' cannot be written to non Apache Iceberg formatted parquet files")

        // If the number of messages is less than the threshold or there are deletes, use JDBC to process
        if (groupOfMessages.length < PipelineEnvironment.values.cdcConfig.writeMessageThreshold.objectStore || containsDeletes.isDefined) {
            val sqlList = groupOfMessages.map { case (_, config, message) =>
                if (message.isInsert)
                    CDCUtil.insertCreateSQL(config, message)
                else if (message.isUpdate)
                    CDCUtil.updateCreateSQL(config, message)
                else
                    CDCUtil.deleteCreateSQL(config, message)
            }
            sqlList.foreach(sql => {
                logger.info("CDC SQL for Athena: " + sql)
                val config = groupOfMessages.head._2
                val glueDbName = config.source.schemaProperties.dbName
                val outputPath = "s3://" + PipelineEnvironment.values.environment + "-temp/athena/" + GuidV5.nameUUIDFrom(Instant.now.toEpochMilli.toString) + ".out"
                ObjectStoreSQLUtil.sql(glueDbName, sql, outputPath)
            })
        }
        else {
            // If the number of messages is greater and there are no deletes, create a file and drop it into the Pipeline
            val messages = groupOfMessages.map(_._3)
            CDCUtil.createFile(config, messages)
        }
    }
}