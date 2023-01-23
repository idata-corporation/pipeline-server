package net.idata.pipeline.model

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

case class DatasetConfig(
                            name: String,
                            source: Source,
                            dataQuality: DataQuality,
                            destination: Destination
                        )

case class Source(
                     schemaProperties: SchemaProperties,
                     fileAttributes: FileAttributes
                 )

case class Destination(
                          schemaProperties: SchemaProperties,
                          database: Database,
                          objectStore: ObjectStore
                      )

case class SchemaProperties(
                               dbName: String,
                               fields: java.util.List[SchemaField]
                           )

case class DataQuality(
                          validateFileHeader: Boolean,
                          deduplicate: Boolean,
                          validationSchema: String
                      )

case class FileAttributes(
                             csvAttributes: CsvAttributes,
                             jsonAttributes: JsonAttributes,
                             xmlAttributes: XmlAttributes
                         )

case class CsvAttributes(
                            delimiter: String,
                            header: Boolean,
                            encoding: String,     // UTF-8, ISO-8859-1, etc]
                        )

case class JsonAttributes(
                             everyRowContainsObject: Boolean,   // If true, each row of the file contains a JSON object
                             encoding: String,     // UTF-8, ISO-8859-1, etc]
                         )

case class XmlAttributes(
                            everyRowContainsObject: Boolean,    // If true, each row of the file contains an XML object
                            encoding: String,     // UTF-8, ISO-8859-1, etc
                        )

case class ObjectStore(
                          prefixKey: String,
                          partitionBy: java.util.List[String],
                          destinationBucketOverride: String,
                          fileFormat: String,
                          writeToTemporaryLocation: Boolean,
                          deleteBeforeWrite: Boolean,
                          manageGlueTableManually: Boolean,
                          useIceberg: Boolean,
                          keyFields: java.util.List[String]
                      )

case class Database(
                       dbName: String, // Database name
                       schema: String,
                       table: String,  // Table name
                       deleteBeforeWrite: Boolean,
                       snowflake: Snowflake,
                       redshift: Redshift
                   )

case class Snowflake(
                        warehouse: String,
                        keyFields: java.util.List[String],
                        formatTypeOptions: java.util.List[String],
                        sqlOverride: String,
                        createSemiStructuredFieldAs: String
                    )

case class Redshift(
                       keyFields: java.util.List[String],
                       useJsonOptions: Boolean
                   )