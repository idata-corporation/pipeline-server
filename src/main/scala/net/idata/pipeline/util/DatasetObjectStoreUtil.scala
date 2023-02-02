package net.idata.pipeline.util

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

import net.idata.pipeline.model.{DatasetConfig, PipelineEnvironment, Schema}
import net.idata.pipeline.util.aws.{GlueUtil, IcebergUtil}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object DatasetObjectStoreUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def createTable(config: DatasetConfig): Unit = {
        val objectStore = config.destination.objectStore
        val locationUrl = {
            if(objectStore.destinationBucketOverride != null)
                "s3://" + PipelineEnvironment.values.environment + "-" + objectStore.destinationBucketOverride + "/" + objectStore.prefixKey + "/" + config.name + "/"
            else
                "s3://" + PipelineEnvironment.values.environment + "-raw-plus/" + objectStore.prefixKey + "/" + config.name + "/"
        }

        val databaseName = config.destination.schemaProperties.dbName
        val tableName = config.name
        if(objectStore.useIceberg) {
            // Iceberg table
            if(IcebergUtil.doesTableExist(databaseName, tableName)) {
                logger.warn("Iceberg table: " + databaseName + "." + tableName + " already exists, not creating or altering the Iceberg table.  To alter an existing table if it has changes, use the ALTER command in Athena or Spark")
            }
            else {
                // Create the table using an Athena query
                logger.info("Creating a new Iceberg: " + databaseName + "." + tableName)
                IcebergUtil.createTable(config, locationUrl)
            }
        }
        else {
            // Non-Iceberg table
            val tableExists = {
                val table = GlueUtil.getTable(databaseName, tableName)
                if(table == null)
                    false
                else
                    true
            }
            if(!tableExists || hasTableChanged(config, locationUrl)) {
                if(tableExists)
                    logger.info("Creating a new version of the Glue table: " + databaseName + "." + tableName)
                else
                    logger.info("Creating a new Glue table: " + databaseName + "." + tableName)
                GlueUtil.createTable(config, locationUrl)
            }
        }
    }

    private def hasTableChanged(config: DatasetConfig, locationUrl: String): Boolean = {
        val currentSchema = GlueUtil.getGlueSchema(config)
        val proposedSchema = Schema(config.destination.schemaProperties.fields)
        val table = GlueUtil.getTable(config.destination.schemaProperties.dbName, config.name)

        if(table.getStorageDescriptor.getLocation.compareToIgnoreCase(locationUrl) != 0)
            true
        else {
            if (proposedSchema.fields.size != currentSchema.fields.size)
                true
            else {
                // Did any of the field names change?
                val newGlueFieldNames = proposedSchema.fields.asScala.map(_.name)
                val currentGlueFieldNames = currentSchema.fields.asScala.map(_.name)
                val diff = newGlueFieldNames.diff(currentGlueFieldNames)
                if (diff.nonEmpty)
                    true
                else {
                    // Did any of the field types change?
                    val newGlueFieldsTypes = proposedSchema.fields.asScala.map(_.`type`)
                    val currentGlueFieldTypes = currentSchema.fields.asScala.map(_.`type`)
                    val diff = newGlueFieldsTypes.diff(currentGlueFieldTypes)
                    if (diff.nonEmpty)
                        true
                    else
                        false
                }
            }
        }
    }
}
