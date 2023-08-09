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
*/

import net.idata.pipeline.model._
import org.quartz.CronExpression

import scala.collection.JavaConverters._

object DatasetValidatorUtil {
    def validate(config: DatasetConfig): Unit = {
        if (config.name == null)
            throw new PipelineException("dataset 'name' is not defined in the JSON")
        if (config.name.length > 80)
            throw new PipelineException("dataset 'name' cannot be greater than 80 characters")

        // Source config
        if (config.source == null)
            throw new PipelineException("dataset 'source' is not defined in the JSON")
        if (config.source.fileAttributes == null && config.source.databaseAttributes == null)
            throw new PipelineException("Either 'source.fileAttributes' or 'source.databaseAttributes must be defined")

        if (config.source.fileAttributes != null && config.source.fileAttributes.unstructuredAttributes != null)
            validateUnstructured(config)
        else
            validateStructuredAndSemiStructured(config)
    }

    private def validateUnstructured(config: DatasetConfig): Unit = {
        if(config.source.fileAttributes.unstructuredAttributes.fileExtension == null)
            throw new PipelineException("For unstructured files, the 'source.fileAttributes.unstructuredAttributes.fileExtension' cannot be null")
        if(config.destination == null)
            throw new PipelineException("For unstructured files, the 'destination' cannot be null and 'objectStore' must also be defined")
        if(config.destination.objectStore == null)
            throw new PipelineException("For unstructured files, the 'destination.objectStore' cannot be null")
        if(config.destination.objectStore.prefixKey == null)
            throw new PipelineException("For unstructured files, the 'destination.objectStore.prefixKey' cannot be null")
        if(config.destination.objectStore.useIceberg)
            throw new PipelineException("The 'iceberg' object store format is not supported for unstructured files")
    }

    private def validateStructuredAndSemiStructured(config: DatasetConfig): Unit = {
        // Source schema properties
        if(config.source.schemaProperties == null)
            throw new PipelineException("'source.schemaProperties' must be defined")

        // Destination config
        if(config.destination == null)
            throw new PipelineException("The 'destination' section must exist")

        // Used to determine if keyFields exist in the schema properties
        val schemaFieldNames = {
            if(config.destination.schemaProperties != null)
                config.destination.schemaProperties.fields.asScala.map(_.name)
            else
                config.source.schemaProperties.fields.asScala.map(_.name)
        }

        // Source database attributes
        if(config.source.databaseAttributes != null) {
            if(config.source.databaseAttributes.`type` == null)
                throw new PipelineException("If 'source.databaseAttributes' is defined, the 'type' field must also be defined")
            if(config.source.databaseAttributes.`type`.compareToIgnoreCase("postgres") != 0)
                throw new PipelineException("The only supported 'databaseAttributes.type' is currently 'postgres'")
            if(config.source.databaseAttributes.postgresSecretsName == null)
                throw new PipelineException("If 'source.databaseAttributes' is defined, the 'postgresSecretsName' field must also be defined")
            if(config.source.databaseAttributes.cronExpression == null)
                throw new PipelineException("If 'source.databaseAttributes' is defined, the 'cronExpression' field must also be defined")
            if(!CronExpression.isValidExpression(config.source.databaseAttributes.cronExpression))
                throw new PipelineException("If 'source.databaseAttributes.cronExpression' is invalid: " + config.source.databaseAttributes.cronExpression)

            if(config.source.databaseAttributes.sqlOverride == null) {
                if(config.source.databaseAttributes.schema == null)
                    throw new PipelineException("If 'source.databaseAttributes' is defined, the 'schema' field must also be defined")
                if(config.source.databaseAttributes.table == null)
                    throw new PipelineException("If 'source.databaseAttributes' is defined, the 'table' field must also be defined")
                if(config.source.databaseAttributes.timestampFieldName == null)
                    throw new PipelineException("If 'source.databaseAttributes' is defined, 'timestampFieldName' must also be defined")
            }
        }

        // Data quality
        if(config.dataQuality != null) {
            if(config.dataQuality.validateFileHeader && config.source.fileAttributes != null && config.source.fileAttributes.csvAttributes == null)
                throw new PipelineException("In the 'dataQuality' section, 'validateFileHeader' = true is only valid for delimited (CSV) files")
            if(config.dataQuality.validationSchema != null) {
                if(config.source.fileAttributes != null && config.source.fileAttributes.jsonAttributes == null && config.source.fileAttributes.xmlAttributes == null)
                    throw new PipelineException("In the 'dataQuality' section, 'validationSchema' is only valid for JSON or XML files")
            }
            if(config.dataQuality.rowRules != null) {
                config.dataQuality.rowRules.asScala.foreach(rule => {
                    if(rule.function == null || rule.function.compareTo("javascript") != 0)
                        throw new PipelineException("In the 'dataQuality.rowRules' section, 'function' must be defined as 'javascript'")
                    if(rule.parameters == null || rule.parameters.asScala.head == null)
                        throw new PipelineException("In the 'dataQuality.rowRules' section, the first parameter must be either the full path " +
                            "to the javascript file or the name of the javascript file (which will need to be placed in the s3://[environment-name]-config/javascript location)")
                })

            }
            if(config.dataQuality.columnRules != null) {
                config.dataQuality.columnRules.asScala.foreach(rule => {
                    if(rule.function == null || rule.function.compareTo("regex") != 0)
                        throw new PipelineException("In the 'dataQuality.columnRules' section, 'function' must be defined as 'regex'")
                    if(rule.parameter == null)
                        throw new PipelineException("In the 'dataQuality.columnRules' section, the 'parameter' should be defined as the regular expression")
                })
            }
        }

        // Transformation
        if(config.transformation != null) {
            if(config.source.fileAttributes != null && config.source.fileAttributes.csvAttributes == null)
                throw new PipelineException("A 'transformation' section is only supported for CSV files")
            if(config.transformation.rowFunctions != null) {
                config.transformation.rowFunctions.forEach(function => {
                    if(function.function.compareTo("javascript") != 0)
                        throw new PipelineException("For the 'transformation.rowFunctions' section, only 'javascript' functions are currently supported")
                    if(function.parameters == null)
                        throw new PipelineException("For the 'transformation.rowFunctions' section, the first 'parameter' must be the name of the javascript file to be processed during transformation")
                })
            }
        }

        // Destination object store
        if(config.destination.objectStore != null) {
            if(config.source.fileAttributes != null && config.source.fileAttributes.csvAttributes == null)
                throw new PipelineException("A destination of 'objectStore' is only supported for CSV files")
            if(config.destination.objectStore.prefixKey == null)
                throw new PipelineException("If the 'destination.objectStore' section is defined, the 'destination.objectStore.prefixKey' must be defined")
            if(config.destination.objectStore.partitionBy != null) {
                config.destination.objectStore.partitionBy.forEach(field => {
                    if(!schemaFieldNames.contains(field))
                        throw new PipelineException("'partitionBy' field name: " + field + " is not in the schema properties for this dataset")
                })
            }
            if(config.destination.objectStore.keyFields != null) {
                config.destination.objectStore.keyFields.forEach(field => {
                    if(!schemaFieldNames.contains(field))
                        throw new PipelineException("'keyFields' field name: " + field + " is not in the schema properties for this dataset")
                })
            }
            if(config.destination.objectStore.fileFormat != null) {
                if(config.destination.objectStore.fileFormat.compareTo("parquet") != 0 && config.destination.objectStore.fileFormat.compareTo("orc") != 0)
                    throw new PipelineException("If the 'destination.objectStore.fileFormat' is defined, it must be either 'parquet' or 'orc'")
            }

            // Get the existing configuration
            val existingConfig = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, config.name)
            if(existingConfig != null) {
                if(existingConfig.destination.objectStore != null) {
                    if(existingConfig.destination.objectStore.partitionBy != null && config.destination.objectStore.partitionBy == null)
                        throw new PipelineException("Cannot change an existing object store dataset from no partition to partitioned.  Delete the Glue table, Dynamodb entries and all S3 data for this dataset first and then re-register")
                    if(existingConfig.destination.objectStore.partitionBy == null && config.destination.objectStore.partitionBy != null)
                        throw new PipelineException("Cannot change an existing object store dataset from partitioned to not partitioned.  Delete the Glue table, Dynamodb entries and all S3 data for this dataset first and then re-register")

                    if(existingConfig.destination.objectStore.useIceberg && !config.destination.objectStore.useIceberg)
                        throw new PipelineException("Cannot change an existing dataset from an 'iceberg' dataset to a non-'iceberg' dataset.  Delete the Glue table, Dynamodb entries and all S3 data for this dataset first and then re-register")
                    if(!existingConfig.destination.objectStore.useIceberg && config.destination.objectStore.useIceberg)
                        throw new PipelineException("Cannot change an existing dataset from a non-'iceberg' dataset to an 'iceberg' dataset.  Delete the Glue table, Dynamodb entries and all S3 for this dataset first and then re-register")
                }
            }
        }

        // Destination database
        if(config.destination.database != null) {
            if(config.destination.database.dbName == null)
                throw new PipelineException("If the 'destination.database' section is defined, the 'destination.database.dbName' must be defined")
            if(config.destination.database.schema == null)
                throw new PipelineException("If the 'destination.database' section is defined, the 'destination.database.schema' must be defined")
            if(config.destination.database.table == null)
                throw new PipelineException("If the 'destination.database' section is defined, the 'destination.database.table' must be defined")

            // Snowflake
            if(config.destination.database.snowflake == null && config.destination.database.redshift == null)
                throw new PipelineException("For the 'destination.database' section, you must define either the redshift or snowflake section")
            if(config.destination.database.snowflake != null) {
                if(config.destination.database.snowflake.warehouse == null)
                    throw new PipelineException("If 'destination.database.snowflake' is defined, you must define the 'warehouse'")
                if(config.destination.database.snowflake.createSemiStructuredFieldAs != null) {
                    val createSemiStructuredFieldsAs = config.destination.database.snowflake.createSemiStructuredFieldAs
                    if(createSemiStructuredFieldsAs.compareToIgnoreCase("VARIANT") != 0 &&
                        createSemiStructuredFieldsAs.compareToIgnoreCase("OBJECT") != 0 &&
                        createSemiStructuredFieldsAs.compareToIgnoreCase("ARRAY") != 0)
                    {
                        throw new PipelineException("destination.database.snowflake.createSemiStructuredFieldAs invalid value.  Valid values are 'VARIANT', 'OBJECT' and 'ARRAY")
                    }
                }
                if(config.source.fileAttributes != null && (config.source.fileAttributes.jsonAttributes != null || config.source.fileAttributes.xmlAttributes != null)) {
                    if(config.destination.database.snowflake.keyFields != null)
                        throw new PipelineException("destination.database.snowflake.keyFields are not supported for JSON or XML source files. You can place the JSON or XML in a column in a CSV file as an alternative.")
                }
                if(config.destination.database.snowflake.keyFields != null) {
                    config.destination.database.snowflake.keyFields.forEach(field => {
                        if(!schemaFieldNames.contains(field))
                            throw new PipelineException("Key field: " + field + " is not in the schema properties for this dataset")
                    })
                }
            }

            // Redshift
            if(config.destination.database.redshift != null) {
                if(config.source.fileAttributes != null && config.source.fileAttributes.xmlAttributes != null)
                    throw new PipelineException("Redshift does not support the ingestion of XML data")
                if(config.destination.database.redshift.keyFields != null) {
                    config.destination.database.redshift.keyFields.forEach(field => {
                        if(!schemaFieldNames.contains(field))
                            throw new PipelineException("Key field: " + field + " is not in the schema properties for this dataset")
                    })
                }
            }
        }

        // Validate semi-structured (JSON, XML)
        if(config.source.fileAttributes != null && (config.source.fileAttributes.jsonAttributes != null || config.source.fileAttributes.xmlAttributes != null))
            validateSemiStructured(config)

        // Validate columns
        validateColumns(sourceSchema = true, config)
        if (config.destination.schemaProperties != null)
            validateColumns(sourceSchema = false, config)
    }

    private def validateColumns(sourceSchema: Boolean, config: DatasetConfig): Unit = {
        val fields = {
            if (sourceSchema)
                config.source.schemaProperties.fields
            else
                config.destination.schemaProperties.fields
        }

        if(fields != null) {
            fields.asScala.foreach(field => {
                if (field.name == null)
                    throw new PipelineException("Column name cannot be null")
                if (!field.name.matches("([A-Za-z0-9\\_]+)"))
                    throw new PipelineException("Column name: " + field.name + " is invalid.  Valid characters are a-z, 0-9 and _")
                if (field.`type` == null ||
                    (
                        field.`type`.compareToIgnoreCase("boolean") != 0 &&
                            field.`type`.compareToIgnoreCase("int") != 0 &&
                            field.`type`.compareToIgnoreCase("tinyint") != 0 &&
                            field.`type`.compareToIgnoreCase("smallint") != 0 &&
                            field.`type`.compareToIgnoreCase("bigint") != 0 &&
                            field.`type`.compareToIgnoreCase("float") != 0 &&
                            field.`type`.compareToIgnoreCase("double") != 0 &&
                            !field.`type`.toLowerCase.startsWith("decimal(") &&
                            field.`type`.compareToIgnoreCase("string") != 0 &&
                            !field.`type`.toLowerCase.startsWith("varchar(") &&
                            !field.`type`.toLowerCase.startsWith("char(") &&
                            field.`type`.compareToIgnoreCase("date") != 0 &&
                            field.`type`.compareToIgnoreCase("timestamp") != 0
                        )
                ) {
                    throw new PipelineException("Invalid field type passed: " + field.`type` + ", supported types include boolean, int, tinyint, smallint, bigint, float, double, decimal(?,?), string, varchar(?), char(?), date, and timestamp")
                }
            })
        }
    }

    private def validateSemiStructured(config: DatasetConfig): Unit = {
        val message = "For JSON and XML datasets, the source schema must have only one field named '_json' or '_xml' according to the source file type, with a field type of 'string'"
        if(config.source.schemaProperties.fields.size != 1)
            throw new PipelineException(message)
        if(config.source.schemaProperties.fields.get(0).`type`.compareToIgnoreCase("string") != 0)
            throw new PipelineException(message)
        if(config.source.fileAttributes != null && config.source.fileAttributes.jsonAttributes != null) {
            if(config.source.schemaProperties.fields.get(0).name.compareToIgnoreCase("_json") != 0)
                throw new PipelineException(message)
        }
        if(config.source.fileAttributes != null && config.source.fileAttributes.xmlAttributes != null) {
            if(config.source.schemaProperties.fields.get(0).name.compareToIgnoreCase("_xml") != 0)
                throw new PipelineException(message)
        }

        if(config.destination.schemaProperties != null) {
            val message = "For JSON and XML datasets, the destination schema must have only one field named '_json' or '_xml' according to the source file type, with a field type of 'string'"
            if(config.destination.schemaProperties.fields.size != 1)
                throw new PipelineException(message)
            if(config.destination.schemaProperties.fields.get(0).`type`.compareToIgnoreCase("string") != 0)
                throw new PipelineException(message)
            if(config.source.fileAttributes != null && config.source.fileAttributes.jsonAttributes != null) {
                if(config.destination.schemaProperties.fields.get(0).name.compareToIgnoreCase("_json") != 0)
                    throw new PipelineException(message)
            }
            if(config.source.fileAttributes != null && config.source.fileAttributes.xmlAttributes != null) {
                if(config.destination.schemaProperties.fields.get(0).name.compareToIgnoreCase("_xml") != 0)
                    throw new PipelineException(message)
            }
        }
    }

    def modify(config: DatasetConfig): DatasetConfig = {
        val sourceSchemaProperties = {
            if(config.source.schemaProperties != null) {
                val fields = config.source.schemaProperties.fields.asScala.map(field => SchemaField(field.name.toLowerCase, field.`type`.toLowerCase)).toList.asJava
                SchemaProperties(config.source.schemaProperties.dbName, fields)
            }
            else
                null
        }

        val destinationSchemaProperties = {
            if (config.destination.schemaProperties != null) {
                // For JSON or XML fields, define 1 field as a string
                if(config.source.fileAttributes != null && config.source.fileAttributes.jsonAttributes != null) {
                    val fields = List(SchemaField("_json", "string")).asJava
                    SchemaProperties(config.destination.schemaProperties.dbName, fields)
                }
                else if(config.source.fileAttributes != null && config.source.fileAttributes.xmlAttributes != null) {
                    val fields = List(SchemaField("_xml", "string")).asJava
                    SchemaProperties(config.destination.schemaProperties.dbName, fields)
                }
                else {
                    val fields = config.destination.schemaProperties.fields.asScala.map(field => SchemaField(field.name.toLowerCase, field.`type`.toLowerCase)).toList.asJava
                    SchemaProperties(config.destination.schemaProperties.dbName, fields)
                }
            }
            else {
                null
            }
        }

        val objectStore = {
            if(config.destination.objectStore == null)
                null
            else {
                val partitionBy = {
                    if(config.destination.objectStore.partitionBy != null)
                        config.destination.objectStore.partitionBy.asScala.map(_.toLowerCase).toList.asJava
                    else
                        null
                }
                val fileFormat = {
                    if(config.source.fileAttributes != null && config.source.fileAttributes.unstructuredAttributes != null)
                        null
                    else if(config.destination.objectStore.fileFormat != null)
                        config.destination.objectStore.fileFormat
                    else
                        "parquet" // default output file format
                }
                val keyFields = {
                    if(config.destination.objectStore.keyFields != null)
                        config.destination.objectStore.keyFields.asScala.map(_.toLowerCase).toList.asJava
                    else
                        null
                }
                val destinationBucketOverride = {
                    if(config.destination.objectStore.destinationBucketOverride != null)
                        config.destination.objectStore.destinationBucketOverride
                    else
                        null
                }
                config.destination.objectStore.copy(
                    prefixKey = config.destination.objectStore.prefixKey.toLowerCase,
                    partitionBy = partitionBy,
                    fileFormat = fileFormat,
                    destinationBucketOverride = destinationBucketOverride,
                    keyFields = keyFields
                )
            }
        }

        val database = {
            // Key fields must be lower case
            if(config.destination.database != null) {
                val (snowflake, redshift)  = {
                    val snowflake = {
                        if(config.destination.database.snowflake != null) {
                            val fields = {
                                if(config.destination.database.snowflake.keyFields != null)
                                    config.destination.database.snowflake.keyFields.asScala.map(_.toLowerCase).toList.asJava
                                else
                                    null
                            }
                            config.destination.database.snowflake.copy(keyFields = fields)
                        }
                        else
                            null
                    }

                    val redshift = {
                        if(config.destination.database.redshift != null) {
                            val fields = {
                                if(config.destination.database.redshift.keyFields != null)
                                    config.destination.database.redshift.keyFields.asScala.map(_.toLowerCase).toList.asJava
                                else
                                    null
                            }
                            config.destination.database.redshift.copy(keyFields = fields)
                        }
                        else
                            null
                    }

                    (snowflake, redshift)
                }
                config.destination.database.copy(snowflake = snowflake, redshift = redshift)
            }
            else
                null
        }

        val fileAttributes = {
            // If we have database attributes, automatically enforce the file attributes
            if(config.source.databaseAttributes != null) {
                val delimiter = {
                    if(config.source.databaseAttributes.outputDelimiter != null)
                        config.source.databaseAttributes.outputDelimiter
                    else
                        ","
                }
                val csvAttributes = CsvAttributes(
                    delimiter = delimiter,
                    header = false,
                    "UTF-8"
                )
                FileAttributes(csvAttributes, null, null, null)
            }
            else
                config.source.fileAttributes
        }

        val source = config.source.copy(schemaProperties = sourceSchemaProperties, fileAttributes = fileAttributes)
        val destination = Destination(destinationSchemaProperties, database, objectStore)

        config.copy(source = source, destination = destination)
    }
}