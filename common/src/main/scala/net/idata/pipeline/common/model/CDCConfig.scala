package net.idata.pipeline.common.model

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

// Message threshold values are used to determine when writing to the data store, if faster to write data to
// a file rather than use JDBC directly to store the data


case class CDCConfig(
                        enabled: Boolean,
                        processMessages: Boolean,
                        publishMessages: Boolean,
                        publishSNSTopicArn: String,
                        writeMessageThreshold: CDCWriteMessageThreshold,
                        debeziumConfig: DebeziumConfig,
                        idataCDCConfig: IDataCDCConfig,
                        datasetMapperTableName: String
                    )
case class CDCWriteMessageThreshold(
                                       objectStore: Int,
                                       redshift: Int,
                                       snowflake: Int
                                   )
