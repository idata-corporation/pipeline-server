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
        finally {
            if(connection != null)
                connection.close()
        }
    }

    private def processTable(connection: Connection, database: String, table: String): String = {
        val storedProcedure = "{EXEC " + "dbo.sp_get_all_cdc_changes_" + table + "(?)}"
        val callableStatement = connection.prepareCall(storedProcedure)

        // Set the start date parameter
        val startDate = getNextDate(database, table)
        callableStatement.setString(1, startDate)

        // execute the stored proc
        callableStatement.execute()
        val resultSet = callableStatement.getResultSet
        val metaData = resultSet.getMetaData
        val columnCount = metaData.getColumnCount

        while(resultSet.next()) {
            val columnMap = 0.until(columnCount).map(i => {
                val name = resultSet.getMetaData.getColumnName(i)
                val value  = resultSet.getString(i)
                (name, value)
            }).toMap

            logger.info("columnMap: " + columnMap.toString)
        }

        null
    }

    private def getNextDate(database: String, table: String): String = {
        "2024-04-04T14:21:00.000"
    }
}
