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
import net.idata.pipeline.model.CDCMessage
import net.idata.pipeline.util.{CDCMessageProcessor, CDCMessagePublisher, CDCUtil}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Connection
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

class MSSqlCDCRunner extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[MSSqlCDCRunner])

    def run(): Unit = {
        try {
            Initialize()

            while (true) {
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

            connection = CDCUtil.getSourceDbConnection

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
            logger.debug("CREATE OR ALTER stored proc: " + "sp_get_all_cdc_changes_" + tableWithSchema)
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
            logger.debug("CREATE OR ALTER stored proc: " + "sp_get_next_lsn_as_date_" + tableWithSchema)
            createStoredProc(connection, sql.toString)

            // create sp_convert_lsn_to_time
            sql.clear()
            sql.append("CREATE OR ALTER PROCEDURE sp_convert_lsn_to_time ")
            sql.append("(@lsn binary(10)) AS BEGIN ")
            sql.append("SELECT sys.fn_cdc_map_lsn_to_time(@lsn) ")
            sql.append("END")
            logger.debug("CREATE OR ALTER stored proc: " + "sp_convert_lsn_to_time")
            createStoredProc(connection, sql.toString)
        }
        catch {
            case e: Exception =>
                throw e
        }
        finally {
            if (connection != null)
                connection.close()
        }
    }

    private def createStoredProc(connection: Connection, sql: String): Unit = {
        val statement = connection.prepareStatement(sql)
        statement.execute()
    }

    private def processTables(): Unit = {
        val tables = PipelineEnvironment.values.cdcConfig.idataCDCConfig.includeTables.asScala.toList

        var connection: Connection = null

        try {
            connection = CDCUtil.getSourceDbConnection
            val database = PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseName

            val f: Future[List[CDCMessage]] = Future {
                tables.flatMap(table => {
                    // Determine the schema name and real table name - table name is formatted as 'schema.table'
                    val array = table.split("\\.")
                    val schema = array(0)
                    val realTable = array(1)

                    new MsSqlCDCRunnerSlave().processTable(connection, database, schema, realTable)
                })
            }

            f.onComplete {
                case Success(cdcMessages) =>
                    // TODO - Order the messages by LSN


                    if (cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.publishMessages) {
                        val thread = new Thread(new CDCMessagePublisher(cdcMessages))
                        thread.start()
                    }
                    if (cdcMessages.nonEmpty && PipelineEnvironment.values.cdcConfig.processMessages) {
                        val thread = new Thread(new CDCMessageProcessor(cdcMessages))
                        thread.start()
                    }

                case Failure(e) =>
                    throw new Exception(Throwables.getStackTraceAsString(e))
            }
        }
        catch {
            case e: Exception =>
                logger.error("Pipeline MsSqlCDCRunner:processTables error: " + Throwables.getStackTraceAsString(e))
        }
        finally {
            if(connection != null)
                connection.close()
        }
    }
}