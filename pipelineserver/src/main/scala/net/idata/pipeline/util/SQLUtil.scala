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

import com.microsoft.sqlserver.jdbc.SQLServerException

import java.sql.{ResultSet, Types}
import scala.collection.mutable.ListBuffer

object SQLUtil {
    def getResultSet(resultSet: ResultSet): List[Map[String, String]] = {
        val rows = new ListBuffer[Map[String, String]]
        val metaData = resultSet.getMetaData
        val columnCount = metaData.getColumnCount

        try {
            while (resultSet.next()) {
                val columnMap = 1.until(columnCount + 1).map(index => {
                    val name = metaData.getColumnName(index)
                    val dataType = metaData.getColumnType(index)
                    val value = dataType match {
                        case Types.BOOLEAN | Types.BIT =>
                            resultSet.getBoolean(index).toString
                        case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
                            resultSet.getInt(index).toString
                        case Types.BIGINT =>
                            resultSet.getLong(index).toString
                        case Types.NUMERIC | Types.DECIMAL =>
                            val value = resultSet.getBigDecimal(index).toString
                            BigDecimal(value).toInt.toString    // Remove scientific notation
                        case Types.REAL =>
                            resultSet.getFloat(index).toString
                        case Types.FLOAT | Types.DOUBLE =>
                            resultSet.getDouble(index).toString
                        case Types.TIME | Types.TIME_WITH_TIMEZONE =>
                            resultSet.getTime(index).toString
                        case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
                            resultSet.getTimestamp(index).toString
                        case Types.DATE =>
                            resultSet.getDate(index).toString
                        case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR =>
                            resultSet.getString(index)
                        case _ =>
                            resultSet.getString(index)
                    }
                    (name, value)
                }).toMap
                rows.append(columnMap)
            }
            rows.toList
        }
        catch {
            case e: SQLServerException =>
                // MSSQL Server CDC bug, ignore this error
                if (e.getMessage.startsWith("An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_"))
                    List[Map[String, String]]()
                else
                    throw e
        }
    }
}
