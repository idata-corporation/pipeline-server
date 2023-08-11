package net.idata.pipeline.util.aws

import com.amazonaws.services.athena.model._
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import net.idata.pipeline.util.ObjectStoreSQLUtility

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

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


