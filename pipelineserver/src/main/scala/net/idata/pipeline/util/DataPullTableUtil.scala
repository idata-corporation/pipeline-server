package net.idata.pipeline.util

import com.google.gson.Gson
import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.NoSQLDbUtil
import net.idata.pipeline.model.DatasetPull
import org.quartz.CronExpression

import java.text.SimpleDateFormat
import java.util.Date

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

case class DatasetPullTable(
                               dataset: String,
                               json: DatasetPull
                           )

object DataPullTableUtil {
    private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    def initialize(dataset: String, cronExpression: String): Unit = {
        val nextPullDate = generateNextPullDate(cronExpression)
        val nextPullDateAsString = dateFormatter.format(nextPullDate)

        val datasetPull = DatasetPull(dataset, nextPullDateAsString, null)
        val gson = new Gson()
        NoSQLDbUtil.putItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset, "json", gson.toJson(datasetPull))
    }

    def deleteEntryIfExists(dataset: String): Unit = {
        // Make sure an entry already exists for the key
        val existing = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset, "json").orNull
        if(existing != null)
            NoSQLDbUtil.deleteItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset)
    }

    def getAll: List[DatasetPull] = {
        val jsonItems = NoSQLDbUtil.getAllItemsAsJSON(PipelineEnvironment.values.dataPullTableName)
        val gson = new Gson()
        jsonItems.map(item => {
            val datasetPullTable = gson.fromJson(item, classOf[DatasetPullTable])
            DatasetPull(datasetPullTable.dataset, datasetPullTable.json.nextPullDate, datasetPullTable.json.lastPullTimestampUsed)
        })
    }

    def update(dataset: String, nextPullDate: Date, lastPullTimestampUsed: String): Unit = {
        val gson = new Gson()

        // Get the existing pull information
        val json = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset, "json").orNull
        if(json == null)
            throw new PipelineException("The table: " + PipelineEnvironment.values.dataPullTableName + " does not contain an entry for the dataset: " + dataset + ", re-register the dataset with the API")
        val datasetPull = gson.fromJson(json, classOf[DatasetPull])

        val newNextPullDate = {
            if(nextPullDate != null)
                dateFormatter.format(nextPullDate)
            else
                datasetPull.nextPullDate
        }
        val newLastPullTimestampUsed = {
            if(lastPullTimestampUsed != null)
                lastPullTimestampUsed
            else
                datasetPull.lastPullTimestampUsed
        }
        val newDatasetPull = DatasetPull(dataset, newNextPullDate, newLastPullTimestampUsed)

        // Write the Dataset pull info DynamoDb
        NoSQLDbUtil.putItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset, "json", gson.toJson(newDatasetPull))
    }

    def getNextPullDate(dataset: String): Date = {
        val json = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.dataPullTableName, "dataset", dataset, "json")
            .getOrElse(throw new PipelineException("The table: " + PipelineEnvironment.values.dataPullTableName + " does not contain an entry for the dataset: " + dataset + ", re-register the dataset with the API"))
        val gson = new Gson()
        val datasetPull = gson.fromJson(json, classOf[DatasetPull])
        dateFormatter.parse(datasetPull.nextPullDate)
    }

    def generateNextPullDate(cronExpression: String): Date = {
        val expression = new CronExpression(cronExpression)
        expression.getNextValidTimeAfter(new Date())
    }
}
