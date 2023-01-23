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

trait NoSQLDbUtility {
    def getItemsKeysByKeyName(tableName: String, keyName: String): List[String]

    def getItemAttribute[T](tableName: String, keyName: String, key: String, attributeName: String): T

    def setItemNameValue(tableName: String, keyName: String, key: String, valueName: String, value: String): Unit

    def getItemJSON(tableName: String, keyName: String, key: String, valueName: String): Option[String]

    def putItemJSON(tableName: String, keyName: String, key: String, valueName: String, value: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit

    def updateItemJSON(tableName: String, keyName: String, key: String, valueName: String, value: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit

    def deleteItemJSON(tableName: String, keyName: String, key: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit

    def queryJSONItemsByKey(tableName: String, keyName: String, key: String): List[String]

    def getAllItemsAsJSON(tableName: String): List[String]

    def getPageOfItemsAsJSON(tableName: String, pageNbr: Int, maxPageSize: Int): List[String]
}
