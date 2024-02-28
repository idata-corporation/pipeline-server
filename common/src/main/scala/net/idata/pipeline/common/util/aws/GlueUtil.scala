package net.idata.pipeline.common.util.aws

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

import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import net.idata.pipeline.common.model._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

    def createTable(databaseName: String, tableName: String, fields: List[SchemaField], partitionBy: List[String], locationUrl: String, fileFormat: String = "parquet", textFileDelimiter: String = ","): Unit = {
        val glue = getGlueClient

        // If the Glue database does not exist, create it
        if(!doesDatabaseExist(databaseName))
            createDatabase(databaseName)

        // Create the Glue table (or a new version if it already exists)
        val tableInput = createTableInput(tableName, fields, partitionBy, locationUrl, fileFormat, textFileDelimiter)
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

    private def createTableInput(tableName: String, fields: List[SchemaField], partitionBy: List[String], locationUrl: String, fileFormat: String, textFileDelimiter: String): TableInput = {
        val (inputFormat, outputFormat, serializationLibrary) = getFileFormats(fileFormat)

        val serdeParameters = new java.util.HashMap[String, String]()
        serdeParameters.put("EXTERNAL", "TRUE")
        if(fileFormat.compareToIgnoreCase("text") == 0) {
            serdeParameters.put("separatorChar", textFileDelimiter)
            serdeParameters.put("field.delim", textFileDelimiter)
            serdeParameters.put("serialization.format", textFileDelimiter)
            serdeParameters.put("quoteChar", "\"")
            serdeParameters.put("escapeChar", "\\")
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

    def glueSchemaToSparkDDL(schema: Schema): String = {
        val fieldList = schema.fields.asScala.map(f => {
            if(f.`type`.toLowerCase.startsWith("struct<") || f.`type`.toLowerCase.startsWith("array<"))
                "`" + f.name + "` " + glueComplexTypeToDDL(f.`type`)
            else if(f.`type`.compareToIgnoreCase("int") == 0)
                "`" + f.name + "`" + " INTEGER"
            else
                "`" + f.name + "` " + f.`type`.toUpperCase
        }).toList
        fieldList.mkString(",")
    }

    private def glueComplexTypeToDDL(`type`: String): String = {
        var index = 0
        val ddl = new ListBuffer[String]()
        var field = new ListBuffer[Char]()

        // Add the rest
        while(index < `type`.length) {
            val character = `type`(index)
            character match {
                case '<' | ' ' =>
                    field += character
                    ddl += field.mkString
                    field = new ListBuffer[Char]()
                case ',' =>
                    field += character
                    field += ' '
                    ddl += field.mkString
                    field = new ListBuffer[Char]()
                case ':' =>
                    val fieldValue = "`" + field.mkString + "`: "
                    ddl += fieldValue
                    field = new ListBuffer[Char]()
                case _ => field += character
            }

            index = index + 1
        }
        if(field.nonEmpty)
            ddl += field.mkString
        ddl.mkString
    }

    private def getGlueClient: AWSGlue = {
        AWSGlueClient.builder().withRegion(PipelineEnvironment.values.region).build()
    }
}
