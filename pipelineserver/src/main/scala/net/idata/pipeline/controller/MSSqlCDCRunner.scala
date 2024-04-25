package net.idata.pipeline.controller

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

import com.google.common.base.Throwables
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.NoSQLDbUtil
import net.idata.pipeline.common.util.aws.SecretsManagerUtil
import net.idata.pipeline.model.CDCMessage
import net.idata.pipeline.util.{CDCMessageProcessor, CDCMessagePublisher, SQLUtil}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import scala.collection.JavaConverters._

class MSSqlCDCRunner extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[MSSqlCDCRunner])

    def run(): Unit = {
        try {
            Initialize()

            while(true) {
                processTables()
                Thread.sleep(PipelineEnvironment.values.cdcConfig.idataCDCConfig.pollingInterval)
            }
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline IDataCDCRunner error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def Initialize(): Unit = {
        val tables = PipelineEnvironment.values.cdcConfig.idataCDCConfig.includeTables.asScala.toList

        // Create the stored procs for each table if they don't already exist
        tables.foreach(table => {
            createStoredProcs(table)
        })
    }

    private def createStoredProcs(table: String): Unit = {
        var connection: Connection = null

        try {
            // Determine the schema name and real table name - table name is formatted as 'schema.table'
            val array = table.split("\\.")
            val schema = array(0)
            val realTable = array(1)
            val tableWithSchema = schema + "_" + realTable

            connection = getDbConnection

            // create sp_get_all_cdc_changes
            val sql = new StringBuilder()
            sql.append("CREATE OR ALTER PROCEDURE sp_get_all_cdc_changes_" + tableWithSchema + " ")
            sql.append("(@start_time nvarchar(50) = NULL) AS BEGIN ")
            sql.append("DECLARE @from_lsn binary(10), @to_lsn binary(10); ")
            sql.append("DECLARE @date DATETIME = CONVERT(DATETIME, @start_time); ")
            sql.append("SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @date); ")
            sql.append("SET @to_lsn = sys.fn_cdc_get_max_lsn(); ")
            sql.append("SELECT * FROM cdc.fn_cdc_get_all_changes_" + tableWithSchema + " (@from_lsn, @to_lsn, N'all update old'); ")
            sql.append("END")
            logger.info("CREATE OR ALTER stored proc: " + "sp_get_all_cdc_changes_" + tableWithSchema)
            createStoredProc(connection, sql.toString)

            // create sp_get_next_lsn_as_date
            sql.clear()
            sql.append("CREATE OR ALTER PROCEDURE sp_get_next_lsn_as_date_" + tableWithSchema + " AS BEGIN ")
            sql.append("DECLARE @from_lsn binary(10), @to_lsn binary(10); ")
            sql.append("DECLARE @max_lsn binary(10); ")
            sql.append("SET @from_lsn = sys.fn_cdc_get_min_lsn ('" + tableWithSchema + "') ")
            sql.append("SET @to_lsn = sys.fn_cdc_get_max_lsn(); ")
            sql.append("SELECT @max_lsn = MAX(__$start_lsn) FROM cdc.fn_cdc_get_all_changes_" + tableWithSchema + " (@from_lsn, @to_lsn, 'all update old'); ")
            sql.append("SELECT sys.fn_cdc_map_lsn_to_time(@max_lsn); ")
            sql.append("END")
            logger.info("CREATE OR ALTER stored proc: " + "sp_get_next_lsn_as_date_" + tableWithSchema)
            createStoredProc(connection, sql.toString)
        }
        catch {
            case e: Exception =>
                throw e
        }
        finally {
            if(connection != null)
                connection.close()
        }
    }

    private def createStoredProc(connection: Connection, sql: String): Unit = {
        val statement = connection.prepareStatement(sql)
        statement.execute()
    }

    private def processTables(): Unit = {
        var connection: Connection = null

        try {
            connection = getDbConnection

            val database = PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseName
            val tables = PipelineEnvironment.values.cdcConfig.idataCDCConfig.includeTables.asScala.toList

            tables.foreach(table => {
                try {
                    // Determine the schema name and real table name - table name is formatted as 'schema.table'
                    val array = table.split("\\.")
                    val schema = array(0)
                    val realTable = array(1)

                    val cdcMessages = processTable(connection, database, schema, realTable)

                    if(cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.publishMessages) {
                        val thread = new Thread(new CDCMessagePublisher(cdcMessages))
                        thread.start()
                    }

                    if(cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.processMessages) {
                        val thread = new Thread(new CDCMessageProcessor(cdcMessages))
                        thread.start()
                    }
                }
                catch {
                    case e: Exception =>
                        logger.error("Pipeline IDataCDCRunner:processTables error: " + Throwables.getStackTraceAsString(e))
                }
            })
        }
        finally {
            if(connection != null)
                connection.close()
        }
    }

    private def processTable(connection: Connection, database: String, schema: String, table: String): List[CDCMessage] = {
        val tableWithSchema = schema + "_" + table
        val storedProcedure = "EXEC " + "dbo.sp_get_all_cdc_changes_" + tableWithSchema + " ?"
        val callableStatement = connection.prepareCall(storedProcedure)

        // Set the start date parameter
        val startDate = getLSNForProc(database, tableWithSchema)
        callableStatement.setString(1, startDate)

        // Execute the stored proc
        callableStatement.execute()
        val resultSet = callableStatement.getResultSet

        // Convert to CDCMessages
        val results = SQLUtil.getResultSet(resultSet)
        val messages = results.flatMap(result => {
            val operation = result.get("__$operation").orNull
            if(operation == null)
                throw new PipelineException("Internal error: CDC value does not have an '__$operation' column")

            val (isBeforeUpdate, isAfterUpdate, isInsert, isDelete) = {
                if(operation.toInt == 4)
                    (false, true, false, false)
                else if(operation.toInt == 3)
                     (true, false, false, false)
                 else if(operation.toInt == 2)
                     (false, false, true, false)
                 else if(operation.toInt == 1)
                     (false, false, false, true)
                 else
                     throw new PipelineException("Internal CDC error, invalid operation value received: " + operation + " for table: " + tableWithSchema)
            }

            if(isAfterUpdate)
                None  // Ignore the 'after' values, already taken care of with the 'isBeforeUpdate'
            else {
                val (before, after) = {
                    // Filter out the system fields
                    val data = result.filterNot(_._1.startsWith("__$"))

                    if(isBeforeUpdate) {
                        // The current record is the 'before'
                        val before = data

                        // Find the 'after' record
                        val seqval = result.get("__$seqval").orNull
                        val after = findAfter(seqval, results)
                        (before.asJava, after.asJava)
                    }
                    else {
                        if(isInsert)
                            (null, data.asJava)
                        else
                            (data.asJava, null)
                    }
                }

                val cdcMessage = CDCMessage(
                    database,
                    schema,
                    table,
                    isInsert,
                    isBeforeUpdate,
                    isDelete,
                    before,
                    after
                )
                logger.info("CDC Message Received: " + cdcMessage.toString)
                Some(cdcMessage)
            }
        })

        storeNextLSNForProc(connection, database, tableWithSchema)

        messages
    }

    private def findAfter(seqval: String, results: List[Map[String, String]]): Map[String, String] = {
        // In the result set, find the 'after' value with a matching 'seqval' and operation of 4
        val after = results.flatMap(r => {
            if(r.getOrElse("__$seqval", "").compareTo(seqval) == 0 && r.getOrElse("__$operation", "").compareTo("4").toInt == 0)
                Some(r)
            else
                None
        }).flatten.toMap

        // Filter out the system fields
        after.filterNot(_._1.startsWith("__$"))
    }

    private def getLSNForProc(database: String, table: String): String = {
        val key = database + "." + table
        val value = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.cdcConfig.idataCDCConfig.lastReadTableName, "name", key, "value").orNull
        if(value == null)
            "2008-01-01T12:00:00.000"   // If it does not exist, use a back date, when CDC was implemented in MSSQL Server
        else
            value.replace("\"", "")
    }

    private def storeNextLSNForProc(connection: Connection, database: String, table: String): Unit = {
        val key = database + "." + table

        // Otherwise, use the proc to get the date
        val storedProcedure = "EXEC " + "dbo.sp_get_next_lsn_as_date_" + table
        val callableStatement = connection.prepareCall(storedProcedure)

        // Execute the stored proc
        callableStatement.execute()
        val resultSet = callableStatement.getResultSet
        if(!resultSet.next())
            throw new PipelineException("Internal error, the stored procedure: " + storedProcedure + " did not return a value")
        val date = resultSet.getString(1)

        // Store the date
        if(date != null)
            NoSQLDbUtil.setItemNameValue(PipelineEnvironment.values.cdcConfig.idataCDCConfig.lastReadTableName, "name", key, "value", date)
    }

    private def getDbConnection: Connection = {
        val secrets = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName))
        val username = secrets.get("username")
        val password = secrets.get("password")
        val jdbcUrl = secrets.get("jdbcUrl")

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        DriverManager.getConnection(jdbcUrl, username, password)
    }
}
