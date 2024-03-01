package net.idata.pipeline.util

import net.idata.pipeline.common.model.DatasetConfig
import net.idata.pipeline.model.DebeziumMessage
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object CDCUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def insert(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()
        val databaseName = config.destination.schemaProperties.dbName
        val columns = message.after.keys.toList

        sql.append("INSERT INTO " + databaseName + "." + config.name + " (" + columns.mkString(", ") + ")")
        sql.append(" VALUES (")

        val values = getValues(config, message.after).mkString(", ")
        sql.append(values + ")")

        sql.toString
    }

    def update(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()
        val databaseName = config.destination.schemaProperties.dbName

        sql.append("UPDATE " + databaseName + "." + config.name)
        sql.append(" SET ")
        val afterColumns = message.after.keys.toList
        val afterValues = getValues(config, message.after)
        val afterValuesWithQuotes = (afterColumns zip afterValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(", ")
        sql.append(afterValuesWithQuotes)

        sql.append(" WHERE ")
        val beforeColumns = message.before.keys.toList
        val beforeValues = getValues(config, message.before)
        val beforeValuesWithQuotes = (beforeColumns zip beforeValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(" AND ")
        sql.append(beforeValuesWithQuotes)

        sql.toString
    }

    def delete(config: DatasetConfig, message: DebeziumMessage): String = {
        val sql = new StringBuilder()
        val databaseName = config.destination.schemaProperties.dbName

        sql.append("DELETE FROM  " + databaseName + "." + config.name)
        sql.append(" WHERE ")
        val beforeColumns = message.before.keys.toList
        val beforeValues = getValues(config, message.before)
        val beforeValuesWithQuotes = (beforeColumns zip beforeValues).map{ case (column, value) =>
            column + " = " + value
        }.mkString(" AND ")
        sql.append(beforeValuesWithQuotes)

        sql.toString
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
