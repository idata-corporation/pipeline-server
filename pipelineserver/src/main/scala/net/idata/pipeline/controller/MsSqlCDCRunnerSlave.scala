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

import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.NoSQLDbUtil
import net.idata.pipeline.model.CDCMessage
import net.idata.pipeline.util.SQLUtil
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Connection
import scala.collection.JavaConverters._

class MsSqlCDCRunnerSlave  {
    private val logger: Logger = LoggerFactory.getLogger(classOf[MsSqlCDCRunnerSlave])

    def processTable(connection: Connection, database: String, schema: String, table: String): List[CDCMessage] = {
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
            val startLSN = result.get("__$start_lsn").orNull

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
                    startLSN,
                    database,
                    schema,
                    table,
                    isInsert,
                    isBeforeUpdate,
                    isDelete,
                    before,
                    after
                )
                logger.debug("CDC Message Received: " + cdcMessage.toString)
                Some(cdcMessage)
            }
        })

        storeNextLSNForProc(connection, database, tableWithSchema)

        messages
    }

    private def findAfter(seqval: String, results: List[Map[String, String]]): Map[String, String] = {
        // In the result set, find the 'after' value with a matching 'seqval' and operation of 4
        val after = results.flatMap(r => {
            if(r.getOrElse("__$seqval", "").compareTo(seqval) == 0 && r.getOrElse("__$operation", "").compareTo("4") == 0)
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

    private def convertLSNtoTimeMillis(connection: Connection, lsn: String): Long = {

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
}