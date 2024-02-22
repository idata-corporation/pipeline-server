package net.idata.pipeline.util

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

import com.google.gson.Gson
import net.idata.pipeline.model._

import java.sql.Timestamp
import java.util.Date
import scala.collection.JavaConverters._

object DatasetStatusUtil {
    def getDatasetStatusSummary(datasetName: String, page: Int): java.util.List[DatasetStatusSummary] = {
        val tableList = NoSQLDbUtil.getPageOfItemsAsJSON(PipelineEnvironment.values.datasetStatusTableName + "-summary", page-1, 20)
        val gson = new Gson
        tableList.map(json => {
            val datasetStatusSummaryTable = gson.fromJson(json, classOf[DatasetStatusSummaryTable])

            // If status is 'processing', calculate the actual time elapsed
            val datasetStatusSummary = datasetStatusSummaryTable.json
            if(datasetStatusSummary.status.compareToIgnoreCase("processing") == 0) {
                val nowInMillis = new Timestamp(new Date().getTime).getTime
                val (elapsed, timedout) = ElapsedTimeUtil.getElapsedTime(nowInMillis - datasetStatusSummary.createdAt)
                val totalTime = {
                    if(timedout)
                        "timed out"
                    else
                        elapsed
                }
                if(timedout)
                    datasetStatusSummary.copy(totalTime = totalTime, status = "error")
                else
                    datasetStatusSummary.copy(totalTime = totalTime)
            }
            else
                datasetStatusSummary
        }).sortWith(_.createdAt > _.createdAt).asJava
    }

    def getDatasetStatus(pipelineToken: String): java.util.List[DatasetStatus] = {
        val tableList = NoSQLDbUtil.queryJSONItemsByKey(PipelineEnvironment.values.datasetStatusTableName, "pipeline_token", pipelineToken)
        val gson = new Gson
        tableList.map(json => {
            val datasetStatusTable = gson.fromJson(json, classOf[DatasetStatusTable])
            datasetStatusTable.json
        }).sortWith(_.epoch < _.epoch).asJava
    }
}
