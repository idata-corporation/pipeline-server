package net.idata.pipeline.controller

import com.google.common.base.Throwables
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.aws.SecretsManagerUtil
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
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
        val secrets = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseSecretName))

        val username = secrets.get("username")
        val password = secrets.get("password")
        val jdbcUrl = secrets.get("jdbcUrl")

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        val connection = DriverManager.getConnection(jdbcUrl, username, password)

        val database = PipelineEnvironment.values.cdcConfig.idataCDCConfig.databaseName
        val tables = PipelineEnvironment.values.cdcConfig.idataCDCConfig.includeTables.asScala.toList

        while (true) {
            tables.foreach(table => {
                try {
                    processTable(connection, database, table)
                }
                catch {
                    case e: Exception =>
                        logger.error("Pipeline IDataCDCRunner:processTables error: " + Throwables.getStackTraceAsString(e))
                }
            })

            Thread.sleep(PipelineEnvironment.values.cdcConfig.idataCDCConfig.pollingInterval)
        }
    }

    private def processTable(connection: Connection, database: String, table: String): String = {
        processTable(connection, database, table)
        val storedProcedure = "{call YourStoredProcedureName(?, ?, ?)}"
        val callableStatement = connection.prepareCall(storedProcedure)

        // Set input parameters if needed
        // callableStatement.setString(1, "parameter1")
        // callableStatement.setInt(2, 123)
        // callableStatement.set...

        // Execute the stored procedure
        callableStatement.execute()

        val resultSet = callableStatement.getResultSet

        // Process result set
        while (resultSet.next()) {
            // Retrieve data from each row
            // Example:
            // val column1Value = resultSet.getString("column1")
            // val column2Value = resultSet.getInt("column2")
            // println(s"Column1: $column1Value, Column2: $column2Value")
        }

        null
    }

    private def getNextDate(database: String, table: String): String = {

    }
}
