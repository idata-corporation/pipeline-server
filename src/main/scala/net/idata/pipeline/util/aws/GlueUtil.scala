package net.idata.pipeline.util.aws

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

import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import net.idata.pipeline.model._

import scala.collection.JavaConverters._

object GlueUtil {
    def getTable(databaseName: String, tableName: String): Table = {
        val glue = getGlueClient
        val getTableRequest = new GetTableRequest()
            .withDatabaseName(databaseName)
            .withName(tableName)

        try {
            glue.getTable(getTableRequest).getTable
        }
        catch {
            case e: EntityNotFoundException =>
                null
        }
    }

    def createTable(config: DatasetConfig, locationUrl: String): Unit = {
        val partitionBy = {
            if(config.destination.objectStore.partitionBy != null)
                config.destination.objectStore.partitionBy.asScala.toList
            else
                null
        }

        createTable(
            config.destination.schemaProperties.dbName,
            config.name,
            config.destination.schemaProperties.fields.asScala.toList,
            partitionBy,
            locationUrl,
            config.destination.objectStore.fileFormat)
    }

    def createTable(databaseName: String, tableName: String, fields: List[SchemaField], partitionBy: List[String], locationUrl: String, fileFormat: String = "parquet", header: Boolean = false, textFileDelimiter: String = ","): Unit = {
        val glue = getGlueClient

        // If the Glue database does not exist, create it
        if(!doesDatabaseExist(databaseName))
            createDatabase(databaseName)

        // Create the Glue table (or a new version if it already exists)
        val tableInput = createTableInput(tableName, fields, partitionBy, locationUrl, fileFormat, header, textFileDelimiter)
        if(getTable(databaseName, tableName) != null) {
            val updateTableRequest = new UpdateTableRequest()
                .withDatabaseName(databaseName)
                .withTableInput(tableInput)
            glue.updateTable(updateTableRequest)
        }
        else {
            val createTableRequest = new CreateTableRequest()
                .withDatabaseName(databaseName)
                .withTableInput(tableInput)
            glue.createTable(createTableRequest)
        }
    }

    def dropTable(databaseName: String, tableName: String): Unit = {
        if(getTable(databaseName, tableName) != null) {
            val glue = getGlueClient
            val deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(databaseName)
                .withName(tableName)
            glue.deleteTable(deleteTableRequest)
        }
    }

    // Get the fields from the table
    def getGlueSchema(config: DatasetConfig): Schema = {
        val glue = getGlueClient

        val glueDatabaseName = config.destination.schemaProperties.dbName

        val getTableRequest = new GetTableRequest().withDatabaseName(glueDatabaseName).withName(config.name)
        val getTableResult = {
            try {
                glue.getTable(getTableRequest)
            }
            catch {
                case e: EntityNotFoundException =>
                    null
            }
        }

        if(getTableResult == null)
            null
        else {
            val glueFields = {
                if(config.destination.objectStore != null && config.destination.objectStore.partitionBy != null) {
                    val gluePartitionFields = getTableResult.getTable.getPartitionKeys.asScala.toList.flatMap(col => {
                        Some(SchemaField(col.getName, col.getType))
                    })
                    val glueFields = getTableResult.getTable.getStorageDescriptor.getColumns.asScala.toList.flatMap(col => {
                        Some(SchemaField(col.getName, col.getType))
                    })
                    (glueFields ::: gluePartitionFields).asJava
                }
                else {
                    getTableResult.getTable.getStorageDescriptor.getColumns.asScala.toList.flatMap(col => {
                        Some(SchemaField(col.getName, col.getType))
                    }).asJava
                }
            }

            Schema(glueFields)
        }
    }

    private def doesDatabaseExist(glueDatabaseName: String): Boolean = {
        val glue = getGlueClient
        val getDatabaseRequest = new GetDatabaseRequest()
            .withName(glueDatabaseName)

        try {
            glue.getDatabase(getDatabaseRequest)
            true
        }
        catch {
            case e: EntityNotFoundException =>
                false
        }
    }

    private def createDatabase(glueSchemaName: String): Unit = {
        val glue = getGlueClient

        val databaseInput =
            new DatabaseInput()
                .withName(glueSchemaName)
                .withDescription("Pipeline Generated Database")
                .withParameters(Map("DatabaseSource" -> "Pipeline").asJava)

        val createDatabaseRequest = new CreateDatabaseRequest()
            .withDatabaseInput(databaseInput)
        glue.createDatabase(createDatabaseRequest)
    }

    private def createTableInput(tableName: String, fields: List[SchemaField], partitionBy: List[String], locationUrl: String, fileFormat: String, header: Boolean, textFileDelimiter: String): TableInput = {
        val (inputFormat, outputFormat, serializationLibrary) = getFileFormats(fileFormat)

        val serdeParameters = new java.util.HashMap[String, String]()
        serdeParameters.put("EXTERNAL", "TRUE")
        if(fileFormat.compareToIgnoreCase("text") == 0) {
            serdeParameters.put("separatorChar", textFileDelimiter)
            serdeParameters.put("field.delim", textFileDelimiter)
            serdeParameters.put("serialization.format", textFileDelimiter)
            serdeParameters.put("quoteChar", "\"")
            serdeParameters.put("escapeChar", "\\")
            if(header)
                serdeParameters.put("skip.header.line.count", "1")
        }
        else if(fileFormat.compareToIgnoreCase("parquet") == 0) {
            serdeParameters.put("serialization.format", "1")
            serdeParameters.put("parquet_compression", "SNAPPY")
        }

        val serdeInfo = new SerDeInfo()
            .withSerializationLibrary(serializationLibrary)
            .withParameters(serdeParameters)

        val columns = fields.map(f => new Column().withName(f.name).withType(f.`type`))

        val partitionColumns = {
            if(partitionBy != null)
                columns.filter(column => partitionBy.contains(column.getName))
            else
                null
        }

        // Create the parameters
        val parameters = new java.util.HashMap[String, String]()
        parameters.put("PipelineGenerated", "true")
        if(fileFormat.compareToIgnoreCase("parquet") == 0)
            parameters.put("classification", "parquet")

        if(partitionColumns == null || partitionColumns.isEmpty) {
            val storageDescriptor = new StorageDescriptor()
                .withColumns(columns.asJava)
                .withLocation(locationUrl)
                .withInputFormat(inputFormat)
                .withOutputFormat(outputFormat)
                .withSerdeInfo(serdeInfo)

            new TableInput().withName(tableName)
                .withStorageDescriptor(storageDescriptor)
                .withParameters(parameters)
                .withTableType("EXTERNAL_TABLE")
        }
        else {
            // Filter out the columns in the partitionBy
            val columnsWithoutPartitionColumns = columns.filterNot(column => partitionColumns.contains(column)).asJava

            val storageDescriptor = new StorageDescriptor()
                .withColumns(columnsWithoutPartitionColumns)
                .withLocation(locationUrl)
                .withInputFormat(inputFormat)
                .withOutputFormat(outputFormat)
                .withSerdeInfo(serdeInfo)

            new TableInput().withName(tableName)
                .withStorageDescriptor(storageDescriptor)
                .withPartitionKeys(partitionColumns:_*)
                .withParameters(parameters)
                .withTableType("EXTERNAL_TABLE")
        }
    }

    private def getFileFormats(fileFormat: String): (String, String, String) = {
        if(fileFormat.compareToIgnoreCase("text") != 0 &&
            fileFormat.compareToIgnoreCase("parquet") != 0 &&
            fileFormat.compareToIgnoreCase("orc") != 0) {
            throw new PipelineException("Invalid fileFormat when creating Glue table: " + fileFormat + ", valid formats include 'text', 'parquet' and 'orc'")
        }

        val inputFormat = {
            if(fileFormat.compareToIgnoreCase("text") == 0)
                "org.apache.hadoop.mapred.TextInputFormat"
            else if(fileFormat.compareToIgnoreCase("parquet") == 0)
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            else
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        }
        val outputFormat = {
            if(fileFormat.compareToIgnoreCase("text") == 0)
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            else if(fileFormat.compareToIgnoreCase("parquet") == 0)
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            else
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        }
        val serializationLibrary = {
            if(fileFormat.compareToIgnoreCase("text") == 0)
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            else if(fileFormat.compareToIgnoreCase("parquet") == 0)
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            else
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        }

        (inputFormat, outputFormat, serializationLibrary)
    }

    private def getGlueClient: AWSGlue = {
        AWSGlueClient.builder().withRegion(PipelineEnvironment.values.region).build()
    }
}
