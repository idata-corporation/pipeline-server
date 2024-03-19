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

import net.idata.pipeline.common.model.{DatasetConfig, PipelineEnvironment}
import net.idata.pipeline.common.util.ObjectStoreUtil
import net.idata.pipeline.model.DebeziumMessage
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._

object CDCUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def insertCreateSQL(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()
        val columns = message.after.asScala.keys.toList

        if(config.destination.objectStore != null)
            sql.append("INSERT INTO " + config.destination.schemaProperties.dbName + "." + config.name + " (" + columns.mkString(", ") + ")")
        if(config.destination.database != null)
            sql.append("INSERT INTO " + config.destination.database.dbName + "."  + config.destination.database.schema + "." + config.destination.database.table + " (" + columns.mkString(", ") + ")")

        sql.append(" VALUES (")

        val values = getValues(config, message.after.asScala.toMap).mkString(", ")
        sql.append(values + ")")

        sql.toString
    }

    def updateCreateSQL(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()

        if(config.destination.objectStore != null)
            sql.append("UPDATE " + config.destination.schemaProperties.dbName + "." + config.name)
        else if(config.destination.database != null)
            sql.append("UPDATE " + config.destination.database.dbName + "."  + config.destination.database.schema + "." + config.destination.database.table)

        sql.append(" SET ")
        val afterColumns = message.after.asScala.keys.toList
        val afterValues = getValues(config, message.after.asScala.toMap)
        val afterValuesWithQuotes = (afterColumns zip afterValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(", ")
        sql.append(afterValuesWithQuotes)

        sql.append(" WHERE ")
        val beforeColumns = message.before.asScala.keys.toList
        val beforeValues = getValues(config, message.before.asScala.toMap)
        val beforeValuesWithQuotes = (beforeColumns zip beforeValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(" AND ")
        sql.append(beforeValuesWithQuotes)

        sql.toString
    }

    def deleteCreateSQL(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()

        if(config.destination.objectStore != null)
            sql.append("DELETE FROM  " + config.destination.schemaProperties.dbName + "." + config.name)
        else if(config.destination.database != null)
            sql.append("DELETE FROM  " + config.destination.database.dbName + "."  + config.destination.database.schema + "." + config.destination.database.table)

        sql.append(" WHERE ")
        val beforeColumns = message.before.asScala.keys.toList
        val beforeValues = getValues(config, message.before.asScala.toMap)
        val beforeValuesWithQuotes = (beforeColumns zip beforeValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(" AND ")
        sql.append(beforeValuesWithQuotes)

        sql.toString
    }

    def createFile(config: DatasetConfig, messages: List[DebeziumMessage]): Unit = {
        val content = createFileInMemory(config, messages)

        // Determine the raw filename
        val rawFilename = {
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH-mm-ss-SSS")
            val date = dateFormat.format(new Date())
            config.name + "." + date + "." + System.currentTimeMillis().toString + ".dataset.csv"
        }
        // Write the data to the raw bucket
        val path = "s3://" + PipelineEnvironment.values.environment + "-raw/temp/" + config.name + "/" + rawFilename
        logger.info("Creating file in the raw bucket: " + path)
        ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(path), ObjectStoreUtil.getKey(path), content)
    }

    private def createFileInMemory(config: DatasetConfig, messages: List[DebeziumMessage]): String = {
        val delimiter = {
            if(config.source.fileAttributes != null && config.source.fileAttributes.csvAttributes != null)
                config.source.fileAttributes.csvAttributes.delimiter
            else
                ","
        }
        val header = {
            if(config.source.fileAttributes != null && config.source.fileAttributes.csvAttributes != null && config.source.fileAttributes.csvAttributes.header)
                messages.head.after.asScala.keys.mkString(delimiter)
            else
                null
        }

        val body = messages.map(message => {
            message.after.asScala.values.mkString(delimiter)
        }).mkString("\n")

        if(header != null)
            header + "\n" + body
        else
            body
    }

    private def getValues(config: DatasetConfig, valueMap: Map[String, String]): List[String] = {
        // Create the values, if text fields add quotes around each value
        valueMap.map { case (fieldName, value) =>
            val schemaField = config.destination.schemaProperties.fields.asScala.find(f => f.name.compareTo(fieldName) == 0).orNull
            schemaField.`type` match {
                case "char" | "varchar" | "string" | "ipaddress" | "date" | "timestamp" =>
                    "'" + value + "'"
                case _ =>
                    value
            }
        }.toList
    }
}
