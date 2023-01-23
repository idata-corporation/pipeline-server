package net.idata.pipeline.model

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