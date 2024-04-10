package net.idata.pipeline.controller

import com.google.common.base.Throwables
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.NoSQLDbUtil
import net.idata.pipeline.common.util.aws.SecretsManagerUtil
import net.idata.pipeline.model.CDCMessage
import net.idata.pipeline.util.{CDCUtil, SQLUtil}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, Types}
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

class IDataCDCRunner extends Runnable {
    private val logger: Logger = LoggerFactory.getLogger(classOf[DebeziumCDCRunner])

    def run(): Unit = {
        try {
            Initialize()
            processTables()
        } catch {
            case e: Exception =>
                throw new PipelineException("Pipeline IDataCDCRunner error: " + Throwables.getStackTraceAsString(e))
        }
    }

    private def Initialize(): Unit = {

    }

    private def processTables(): Unit = {
        var connection: Connection = null

        val secrets = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName))

        try {
            val username = secrets.get("username")
            val password = secrets.get("password")
            val jdbcUrl = secrets.get("jdbcUrl")

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
            connection = DriverManager.getConnection(jdbcUrl, username, password)

            val database = PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseName
            val tables = PipelineEnvironment.values.cdcConfig.idataCDCConfig.includeTables.asScala.toList

            while (true) {
                tables.foreach(table => {
                    try {
                        // Determine the schema name and real table name - table name is formatted as 'schema.table'
                        val array = table.split("\\.")
                        val schema = array(0)
                        val realTable = array(1)

                        val cdcMessages = processTable(connection, database, schema, realTable)

                        if(PipelineEnvironment.values.cdcConfig.publishMessages)
                            CDCUtil.processMessages(cdcMessages)
                    }
                    catch {
                        case e: Exception =>
                            logger.error("Pipeline IDataCDCRunner:processTables error: " + Throwables.getStackTraceAsString(e))
                    }
                })

                Thread.sleep(PipelineEnvironment.values.cdcConfig.idataCDCConfig.pollingInterval)
            }
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
        val startDate = getNextDate(connection, database, tableWithSchema)
        callableStatement.setString(1, startDate)

        // Execute the stored proc
        callableStatement.execute()
        val resultSet = callableStatement.getResultSet

        // Convert to CDCMessages
        val results = SQLUtil.getResultSet(resultSet)
        results.map(result => {
            val operation = result.get("__$operation").orNull
            if(operation == null)
                throw new PipelineException("Internal error: CDC value does not have an '__$operation' column")

            val (isInsert, isUpdate, isDelete) = {
                 if(operation.toInt == 4)
                     (true, false, false)
                 else if(operation.toInt == 3)
                     (false, true, false)
                 else
                     (false, false, false)
            }

            // Strip out the system fields
            val data = result.filterNot(_._1.startsWith("__$"))

            val (before, after) = {
                if(isUpdate || isInsert)
                    (null, data.asJava)
                else
                    (data.asJava, null)
            }

            CDCMessage(
                database,
                schema,
                table,
                isInsert,
                isUpdate,
                isDelete,
                before,
                after
            )
        })
    }

    private def getNextDate(connection: Connection, database: String, table: String): String = {
        val key = database + "." + table
        val value = {
            try {
                NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.cdcConfig.idataCDCConfig.lastReadTableName, "name", key, "value").orNull
            } catch {
                case _: Exception => null
            }
        }

        val date = {
            // If this is the first time calling the procedure, use a 2008 date
            if(value == null)
                "2008-01-01T12:00:00.000"
            else {
                // Otherwise, use the proc to get the date
                val storedProcedure = "EXEC " + "dbo.sp_get_next_lsn_as_date_" + table
                val callableStatement = connection.prepareCall(storedProcedure)

                // Execute the stored proc
                callableStatement.execute()
                val resultSet = callableStatement.getResultSet
                if(!resultSet.next())
                    throw new PipelineException("Internal error, the stored procedure: " + storedProcedure + " did not return a value")
                resultSet.getString(1)
            }
        }

        // Store the date
        NoSQLDbUtil.setItemNameValue(PipelineEnvironment.values.cdcConfig.idataCDCConfig.lastReadTableName, "name", key, "value", date)
        date
    }
}
