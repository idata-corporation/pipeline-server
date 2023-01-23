package net.idata.pipeline.util

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
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
