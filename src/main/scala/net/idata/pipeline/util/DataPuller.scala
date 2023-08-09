package net.idata.pipeline.util

import net.idata.pipeline.model._
import net.idata.pipeline.util.aws.SecretsManagerUtil
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Types}
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConverters._

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

class DataPuller {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def run(): Unit = {
        DataPullTableUtil.getAll.foreach(datasetPull => {
            val nextPullDate = DataPullTableUtil.getNextPullDate(datasetPull.dataset)

            // Attempt a pull?
            val now = new Date()
            if(now.compareTo(nextPullDate) > 0) {
                val config = DatasetConfigIO.read(PipelineEnvironment.values.datasetTableName, datasetPull.dataset)

                // Before we pull the data, save the actual pull data date and generate the next pull date from the cron expression
                val actualDataPullDate = new Date()
                val generatedNextPullDate = DataPullTableUtil.generateNextPullDate(config.source.databaseAttributes.cronExpression)

                val data = pull(config, datasetPull)
                if(data == null) {
                    // Re-initialize the data pull table to reset the next pull date request
                    DataPullTableUtil.update(config.name, generatedNextPullDate, null)
                }
                else {
                    // Re-initialize the data pull table to reset the next pull date request and the last pull date
                    DataPullTableUtil.update(config.name, generatedNextPullDate, actualDataPullDate)

                    // Write the data to the raw bucket
                    val rawFilename = {
                        val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH-mm-ss-SSS")
                        val date = dateFormat.format(new Date())
                        config.name + "." + date + "." + System.currentTimeMillis().toString + ".dataset.csv"
                    }
                    val path = "s3://" + PipelineEnvironment.values.environment + "-raw/temp/" + config.name + "/" + rawFilename
                    ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(path), ObjectStoreUtil.getKey(path), data)
                }
            }
        })
    }

    private def pull(config: DatasetConfig, datasetPull: DatasetPull): String = {
        logger.info("Attempting to pull data for dataset: " + config.name)
        val databaseAttributes = config.source.databaseAttributes

        val connection = getDatabaseConnection(databaseAttributes)
        val rows = new util.ArrayList[String]()

        try {
            val sql = new StringBuilder()
            if(databaseAttributes.sqlOverride != null) {
                sql.append(databaseAttributes.sqlOverride)
            }
            else {
                val fieldNames = {
                    if (databaseAttributes.includeFields == null)
                        config.source.schemaProperties.fields.asScala.map(_.name).toList
                    else
                        databaseAttributes.includeFields.asScala
                }
                sql.append("select ")
                sql.append(fieldNames.mkString(","))
                sql.append(" from ")
                sql.append(databaseAttributes.table)
                if (datasetPull.lastPullTimestampUsed != null) {
                    sql.append(" where ")
                    sql.append(databaseAttributes.timestampFieldName + " > '" + datasetPull.lastPullTimestampUsed + "'")
                }
            }

            // Do the query
            logger.info("For dataset: " + config.name + ", pull data query: " + sql.mkString)
            val preparedStatement = connection.prepareStatement(sql.mkString)
            val resultSet = preparedStatement.executeQuery()
            val outputDelimiter = {
                if(databaseAttributes.outputDelimiter == null)
                    ","
                else
                    databaseAttributes.outputDelimiter
            }

            val resultSetMetadata = resultSet.getMetaData
            while(resultSet.next()) {
                val row = (1 until resultSetMetadata.getColumnCount + 1).toList.map(index => {
                    val dataType = resultSetMetadata.getColumnType(index)
                    dataType match {
                        case Types.BOOLEAN =>
                            resultSet.getBoolean(index).toString
                        case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
                            resultSet.getInt(index).toString
                        case Types.BIGINT | Types.NUMERIC =>
                            resultSet.getLong(index).toString
                        case Types.CHAR | Types.VARCHAR =>
                            resultSet.getString(index)
                        case Types.FLOAT =>
                            resultSet.getFloat(index).toString
                        case Types.DECIMAL | Types.DOUBLE | Types.REAL =>
                            resultSet.getDouble(index).toString
                        case Types.TIME | Types.TIME_WITH_TIMEZONE =>
                            resultSet.getTime(index).toString
                        case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
                            resultSet.getTimestamp(index).toString
                        case Types.DATE =>
                            resultSet.getDate(index).toString
                        case _ =>
                            throw new PipelineException("Data type: " + resultSetMetadata.getColumnTypeName(index) + " is not currently supported, please contact customer support")
                    }
                })
                val rowWithDelimiter = row.mkString(outputDelimiter)
                rows.add(rowWithDelimiter)
            }
        }
        finally {
            connection.close()
        }

        if(rows.size() == 0)
            null
        else
            rows.asScala.mkString("\n")
    }

    private def getDatabaseConnection(databaseAttributes: DatabaseAttributes): Connection = {
        // Grab the secrets
        val (secrets, secretsName) = {
            if(databaseAttributes.postgresSecretsName != null) {
                Class.forName("org.postgresql.Driver")

                val secrets = SecretsManagerUtil.getSecretMap(databaseAttributes.postgresSecretsName)
                    .getOrElse(throw new PipelineException("Secrets not found for secret name: " + databaseAttributes.postgresSecretsName))
                (secrets, databaseAttributes.postgresSecretsName)
            }
            else {
                throw new PipelineException("")
            }
        }

        val jdbcUrl = secrets.get("jdbcUrl")
        if(jdbcUrl == null)
            throw new PipelineException("The 'jdbcUrl' does not exist in the Secrets Manager secrets: " + secretsName)
        val username = secrets.get("username")
        if(username == null)
            throw new PipelineException("The 'username' does not exist in the Secrets Manager secrets: " + secretsName)
        val password = secrets.get("password")
        if(password == null)
            throw new PipelineException("The 'password' does not exist in the Secrets Manager secrets: " + secretsName)

        DriverManager.getConnection(jdbcUrl, username, password)
    }
}
