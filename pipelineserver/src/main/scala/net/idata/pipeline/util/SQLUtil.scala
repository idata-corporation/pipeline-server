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
                            convertIfNull(resultSet.getBoolean(index))
                        case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
                            convertIfNull(resultSet.getInt(index))
                        case Types.BIGINT =>
                            convertIfNull(resultSet.getLong(index))
                        case Types.NUMERIC | Types.DECIMAL =>
                            val value = convertIfNull(resultSet.getBigDecimal(index))
                            if(value.isBlank)
                                value
                            else
                                BigDecimal(value).toInt.toString    // Remove scientific notation
                        case Types.REAL =>
                            convertIfNull(resultSet.getFloat(index))
                        case Types.FLOAT | Types.DOUBLE =>
                            convertIfNull(resultSet.getDouble(index))
                        case Types.TIME | Types.TIME_WITH_TIMEZONE =>
                            convertIfNull(resultSet.getTime(index))
                        case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
                            convertIfNull(resultSet.getTimestamp(index))
                        case Types.DATE =>
                            convertIfNull(resultSet.getDate(index))
                        case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR =>
                            convertIfNull(resultSet.getString(index))
                        case _ =>
                            convertIfNull(resultSet.getString(index))
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

    private def convertIfNull(value: Any): String = {
        if(value == null)
            ""
        else
            value.toString
    }
}
