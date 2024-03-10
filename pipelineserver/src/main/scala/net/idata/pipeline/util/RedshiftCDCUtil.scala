package net.idata.pipeline.util

import net.idata.pipeline.common.model.DatasetConfig
import net.idata.pipeline.model.DebeziumMessage
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import java.util.Properties

class RedshiftCDCUtil {
    private val logger: Logger = LoggerFactory.getLogger(classOf[RedshiftCDCUtil])
    private val messageThreshold = 30

    def process(groupOfMessages: List[(String, DatasetConfig, DebeziumMessage)]): Unit = {
        val containsDeletes = groupOfMessages.find(_._3.isDelete == true)

        // If the number of messages is less than the threshold or there are deletes, use JDBC to process
        if (groupOfMessages.length < messageThreshold || containsDeletes.isDefined) {
            val sql = groupOfMessages.map { case (_, config, message) =>
                if (message.isInsert)
                    CDCUtil.insertCreateSQL(config, message)
                else if (message.isUpdate)
                    CDCUtil.updateCreateSQL(config, message)
                else
                    CDCUtil.deleteCreateSQL(config, message)
            }.mkString(";\n")

            logger.info("CDC SQL for Redshift: " + sql)
            executeSQL(sql)
        }
        else {
            // If the number of messages is greater and there are no deletes, create a file and drop it into the Pipeline
            val config = groupOfMessages.head._2
            val messages = groupOfMessages.map(_._3)
            CDCUtil.createFile(config, messages)
        }
    }

    private def executeSQL(sql: String): Unit = {
        // TODO - might want to move the secrets acquisition to another class, since we don't need the JobContext here
        val secrets = SecretsUtil.redshiftSecrets()

        Class.forName("com.amazon.redshift.jdbc42.Driver")

        var conn: Connection = null
        var statement: java.sql.Statement = null

        try {
            val properties = new Properties()
            properties.setProperty("user", secrets.username)
            properties.setProperty("password", secrets.password)
            conn = DriverManager.getConnection(secrets.jdbcUrl, properties)

            statement = conn.createStatement()
            statement.execute(sql)
        } finally {
            if (statement != null)
                statement.close()
            if (conn != null)
                conn.close()
        }
    }
}
