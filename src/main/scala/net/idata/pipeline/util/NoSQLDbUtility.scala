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
