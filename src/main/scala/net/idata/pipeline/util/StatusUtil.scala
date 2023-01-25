package net.idata.pipeline.util

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

import com.google.gson.Gson
import net.idata.pipeline.model._
import org.slf4j.{Logger, LoggerFactory}

import java.security.InvalidParameterException
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

class StatusUtil {
    private val logger: Logger = LoggerFactory.getLogger(classOf[StatusUtil])
    private var tableName: String = _
    private var processName: Option[String] = None
    private var pipelineToken: Option[String] = None
    private var publisherToken: Option[String] = None
    private var filename: Option[String] = None

    def init(tableName: String, processName: String): StatusUtil = {
        this.tableName = tableName
        this.processName = Some(processName)
        this
    }

    def overrideProcessName(processName: String): Unit = {
        this.processName = Some(processName)
    }

    def setPipelineToken(pipelineToken: String): Unit = {
        if(pipelineToken != null)
            this.pipelineToken = Some(pipelineToken)
    }

    def setPublisherToken(publisherToken: String): Unit = {
        if(publisherToken != null)
            this.publisherToken = Some(publisherToken)
    }

    def setFilename(filename: String): Unit = {
        if(filename != null)
            this.filename = Some(filename)
    }

    def setFilename(metadata: DatasetMetadata): Unit = {
        this.filename = Some(metadata.dataFileName)
    }

    def info(state: String, description: String): Unit = {
        send(state, "info", description)
    }

    def warn(state: String, description: String): Unit = {
        send(state, "warning", description)
    }

    def error(state: String, description: String): Unit = {
        send(state, "error", description)
    }

    private def send(state: String, code: String, description: String): Unit = {
        // Only output to the queue if the pipeline token is not null
        if(this.pipelineToken.isDefined) {
            state match {
                case "begin" | "processing" | "end" =>
                case _ => throw new InvalidParameterException("Invalid state. State must be one of the following: begin, processing, end")
            }
            code match {
                case "info" | "warning" | "error" =>
                case _ => throw new InvalidParameterException("Invalid code.  Code must be one of the following: info, warning, error")
            }

            val status = Status(processName.getOrElse(""),
                publisherToken.getOrElse(""),
                pipelineToken.getOrElse(""),
                filename.getOrElse(""),
                state,
                code,
                description)

            writeToNoSQLDb(status)
        }

        // Write to the logger
        code match {
            case "info" =>
                if(state.compareTo("processing") == 0)
                    logger.info(description)
            case "warning" =>
                if(state.compareTo("processing") == 0)
                    logger.warn(description)
            case "error" =>
                if(state.compareTo("processing") == 0)
                    logger.error(description)
        }
    }

    private def writeToNoSQLDb(status: Status): Unit = {
        val gson = new Gson
        val nowTimestamp = new Timestamp(new Date().getTime)
        val nowInMillis = new Timestamp(new Date().getTime).getTime

        val dateFormat = "MM-dd-yyyy HH:mm:ss z"
        val datasetName = getDatasetName(status.filename, status.pipelineToken)

        // Query for the pipeline token in the dataset status summary table
        val statusSummaryList = NoSQLDbUtil.queryJSONItemsByKey(tableName + "-summary", "pipeline_token", status.pipelineToken)

        if(statusSummaryList != null && statusSummaryList.nonEmpty) {
            // If the summary record exists, update it
            val datasetStatusSummaryTable = gson.fromJson(statusSummaryList.head, classOf[DatasetStatusSummaryTable])
            val datasetStatusSummary = datasetStatusSummaryTable.json

            val (elapsed, timedout) = ElapsedTimeUtil.getElapsedTime(nowInMillis - datasetStatusSummary.createdAt)
            val totalTime = {
                if(timedout)
                    "timed out"
                else
                    elapsed
            }

            val (error, warning) = {
                val warning = datasetStatusSummary.status.compareTo("warning") == 0
                val error = datasetStatusSummary.status.compareTo("error") == 0
                (error, warning)
            }

            val running = {
                if (error)
                    false
                else if (timedout)
                    false
                else if (status.processName.compareToIgnoreCase("CopyUnstructuredController") == 0
                    && status.state.compareToIgnoreCase("end") == 0)
                    false
                else if (status.processName.compareToIgnoreCase("JobRunner") == 0
                    && status.state.compareToIgnoreCase("end") == 0)
                    false
                else if (status.processName.compareToIgnoreCase("SnowflakeLoader") == 0
                    && status.state.compareToIgnoreCase("end") == 0)
                    false
                else if (status.processName.compareToIgnoreCase("RedshiftLoader") == 0
                    && status.state.compareToIgnoreCase("end") == 0)
                    false
                else if (status.processName.compareToIgnoreCase("Trigger") == 0
                    && status.state.compareToIgnoreCase("end") == 0)
                    false
                else
                    true
            }

            val statusString = {
                if (timedout)
                    "error"
                else if (running)
                    "processing"
                else if (error)
                    "error"
                else if (warning)
                    "warning"
                else
                    "success"
            }

            val statusSummary = DatasetStatusSummary(
                datasetStatusSummary.createdAtTimestamp,
                datasetStatusSummary.createdAt,
                nowInMillis,
                datasetName,
                pipelineToken.orNull,
                processName.orNull,
                new SimpleDateFormat(dateFormat).format(Timestamp.valueOf(datasetStatusSummary.createdAtTimestamp)),
                new SimpleDateFormat(dateFormat).format(nowTimestamp),
                totalTime,
                statusString
            )

            NoSQLDbUtil.updateItemJSON(tableName + "-summary",
                "pipeline_token",
                pipelineToken.orNull,
                "json",
                gson.toJson(statusSummary),
                "created_at",
                datasetStatusSummaryTable.created_at
            )
        }
        else {
            // Summary record does not exist, create it
            val statusSummary = DatasetStatusSummary(
                nowTimestamp.toString,
                nowInMillis,
                nowInMillis,
                datasetName,
                pipelineToken.orNull,
                processName.orNull,
                new SimpleDateFormat(dateFormat).format(nowTimestamp),
                new SimpleDateFormat(dateFormat).format(nowTimestamp),
                "0 seconds",
                "processing"
            )

            NoSQLDbUtil.putItemJSON(tableName + "-summary",
                "pipeline_token", pipelineToken.orNull,
                "json",
                gson.toJson(statusSummary),
                "created_at",
                nowInMillis
            )
        }

        // Save the dataset status record
        val datasetStatus = DatasetStatus(
            0,
            new SimpleDateFormat(dateFormat).format(nowTimestamp),
            datasetName,
            status.processName,
            status.publisherToken,
            status.pipelineToken,
            status.filename,
            status.state,
            status.code,
            status.description,
            nowInMillis
        )

        NoSQLDbUtil.putItemJSON(tableName,
            "pipeline_token", pipelineToken.orNull,
            "json",
            gson.toJson(datasetStatus),
            "created_at",
            nowInMillis
        )
    }

    private def getDatasetName(filename: String, pipelineToken: String): String = {
        if(filename != null && filename.contains(".dataset.")) {
            val tokens = filename.split("\\.")
            tokens(0)
        }
        else if(pipelineToken != null) {
            // metadata.json file ingestion
            val jsonMetadata = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.archivedMetadataTableName, "pipeline_token", pipelineToken, "metadata").getOrElse(
                throw new PipelineException("Internal error, pipelineToken: " + pipelineToken + " was not found in the DynamoDb table")
            )
                .replaceAll("\\\\", "")
                .drop(1)
                .dropRight(1)
            val gson = new Gson
            val metadata = gson.fromJson(jsonMetadata, classOf[DatasetMetadata])
            metadata.dataset
        }
        else
            null
    }
}

object StatusUtil {
    private var _statusUtil: StatusUtil = _
    def init(tableName: String, processName: String): Unit =
        _statusUtil  = new StatusUtil().init(tableName, processName)

    def overrideProcessName(processName: String): Unit =
        _statusUtil.overrideProcessName(processName)

    def setPipelineToken(pipelineToken: String): Unit =
        _statusUtil.setPipelineToken(pipelineToken)

    def setPublisherToken(publisherToken: String): Unit =
        _statusUtil.setPublisherToken(publisherToken)

    def setFilename(filename: String): Unit =
        _statusUtil.setFilename(filename)

    def setFilename(metadata: DatasetMetadata): Unit =
        _statusUtil.setFilename(metadata)

    def info(state: String, description: String): Unit =
        _statusUtil.send(state, "info", description)

    def warn(state: String, description: String): Unit =
        _statusUtil.send(state, "warning", description)

    def error(state: String, description: String): Unit =
        _statusUtil.send(state, "error", description)
}