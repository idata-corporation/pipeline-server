package net.idata.pipeline.util.aws

import com.amazonaws.services.athena.model._
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import net.idata.pipeline.util.ObjectStoreSQLUtility

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

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


class AthenaUtil(val athena: AmazonAthena) extends ObjectStoreSQLUtility {
    override def sql(sql: String, outputPath: String): Option[String] = {
        val queryExecutionId = doQuery(sql, outputPath)
        waitForQuery(queryExecutionId)
    }

    override def sql(databaseName: String, sql: String, outputPath: String): Option[String] = {
        val queryExecutionId = doQueryWithDatabase(databaseName, sql, outputPath)
        waitForQuery(queryExecutionId)
    }

    private def doQuery(sql: String, outputPath: String): String = {
        // Start the query execution
        val resultConfiguration = new ResultConfiguration().withOutputLocation(outputPath)
        val startQueryExecutionRequest = new StartQueryExecutionRequest()
            .withQueryString(sql)
            .withResultConfiguration(resultConfiguration)
        val startQueryExecutionResult = athena.startQueryExecution(startQueryExecutionRequest)
        startQueryExecutionResult.getQueryExecutionId
    }

    private def doQueryWithDatabase(databaseName: String, sql: String, outputPath: String): String = {
        // Start the query execution
        val queryExecutionContext = new QueryExecutionContext().withDatabase(databaseName)
        val resultConfiguration = new ResultConfiguration().withOutputLocation(outputPath)
        val startQueryExecutionRequest = new StartQueryExecutionRequest()
            .withQueryString(sql)
            .withQueryExecutionContext(queryExecutionContext)
            .withResultConfiguration(resultConfiguration)
        val startQueryExecutionResult = athena.startQueryExecution(startQueryExecutionRequest)
        startQueryExecutionResult.getQueryExecutionId
    }

    private def waitForQuery(queryExecutionId: String): Option[String] = {
        var outputFile:Option[String] = None
        val getQueryExecutionRequest = new GetQueryExecutionRequest().withQueryExecutionId(queryExecutionId)
        breakable {
            while(true) {
                val getQueryExecutionResult = athena.getQueryExecution(getQueryExecutionRequest)
                outputFile = Some(getQueryExecutionResult.getQueryExecution.getResultConfiguration.getOutputLocation)

                val queryState = getQueryExecutionResult.getQueryExecution.getStatus.getState
                if (queryState.equals(QueryExecutionState.FAILED.toString))
                    throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution.getStatus.getStateChangeReason)
                else if (queryState.equals(QueryExecutionState.CANCELLED.toString))
                    throw new RuntimeException("Query was cancelled")
                else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString))
                    break

                Thread.sleep(1000)
            }
        }
        outputFile
    }

    private def getResults(queryExecutionId: String): List[Row] = {
        val results = new ListBuffer[Row]()
        val getQueryResultsRequest = new GetQueryResultsRequest()
            .withMaxResults(1000)
            .withQueryExecutionId(queryExecutionId)
        var getQueryResultsResult = athena.getQueryResults(getQueryResultsRequest)
        val columnInfoList:java.util.List[ColumnInfo] = getQueryResultsResult.getResultSet.getResultSetMetadata.getColumnInfo
        breakable {
            while(true) {
                val queryResults:List[Row] = getQueryResultsResult.getResultSet.getRows.asScala.toList
                queryResults.foreach(row => results.append(row))

                // If nextToken is null, there are no more pages to read. Break out of the loop.
                if (getQueryResultsResult.getNextToken == null)
                    break
                getQueryResultsResult = athena.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken))
            }
        }

        results.toList
    }
}

object AthenaUtilBuilder {
    private lazy val athena = AmazonAthenaClientBuilder.defaultClient

    def build(): ObjectStoreSQLUtility = new AthenaUtil(athena)
}


