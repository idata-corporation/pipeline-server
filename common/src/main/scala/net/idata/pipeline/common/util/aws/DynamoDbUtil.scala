package net.idata.pipeline.common.util.aws

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

import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import net.idata.pipeline.common.model.PipelineException
import net.idata.pipeline.common.util.NoSQLDbUtility

import java.util
import scala.collection.JavaConverters._

class DynamoDbUtil(dynamoDB: DynamoDB, amazonDynamoDb: AmazonDynamoDB) extends NoSQLDbUtility {
    override def getItemsKeysByKeyName(tableName: String, keyName: String): List[String] = {
        val table = dynamoDB.getTable(tableName)
        val itemCollection = table.scan()
        val items = itemCollection.asScala.map(item => item.asMap())
        items.map(item => item.get(keyName).asInstanceOf[String]).toList
    }

    override def getItemAttribute[T](tableName: String, keyName: String, key: String, attributeName: String): T = {
        val table = dynamoDB.getTable(tableName)
        if (table == null) throw new PipelineException("Configuration error: the table 'dataset' was not found in DynamoDB")

        val item = table.getItem(keyName, key)
        item.get(attributeName).asInstanceOf[T]
    }

    override def setItemNameValue(tableName: String, keyName: String, key: String, valueName: String, value: String): Unit = {
        val table = dynamoDB.getTable(tableName)

        import com.amazonaws.services.dynamodbv2.document.Item

        val item = new Item()
            .withPrimaryKey(keyName, key)
            .withString(valueName, value)

        table.putItem(item)
    }

    override def getItemJSON(tableName: String, keyName: String, key: String, valueName: String): Option[String] = {
        val table = dynamoDB.getTable(tableName)
        if (table == null)
            throw new PipelineException("Configuration error: the table " + tableName + " was not found in DynamoDB")

        val getItemSpec = new GetItemSpec()
            .withPrimaryKey(keyName, key)
            .withConsistentRead(true)
        val item = table.getItem(getItemSpec)

        if(item == null)
            None
        else if(valueName == null)
            Some(item.toJSON)
        else
            Some(item.getJSON(valueName))
    }

    override def putItemJSON(tableName: String, keyName: String, key: String, valueName: String, value: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit = {
        val table = dynamoDB.getTable(tableName)

        val map = new util.HashMap[String, Object]
        map.put(valueName, value)

        if(sortKeyName != null) {
            table.putItem(new Item()
                .withPrimaryKey(keyName, key)
                .withNumber(sortKeyName, sortKeyValue)
                .withJSON(valueName, value))
        }
        else {
            table.putItem(new Item()
                .withPrimaryKey(keyName, key)
                .withJSON(valueName, value))
        }
    }

    override def updateItemJSON(tableName: String, keyName: String, key: String, valueName: String, value: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit = {
        val table = dynamoDB.getTable(tableName)

        val updateItemSpec = {
            if(sortKeyName != null) {
                new UpdateItemSpec()
                    .withPrimaryKey(keyName, key, sortKeyName, sortKeyValue)
                    .withUpdateExpression("set " + valueName + " = :val")
                    .withValueMap(new ValueMap()
                        .withJSON(":val", value))
            }
            else {
                new UpdateItemSpec()
                    .withPrimaryKey(keyName, key)
                    .withUpdateExpression("set " + valueName + " = :val")
                    .withValueMap(new ValueMap()
                        .withJSON(":val", value))
            }
        }

        table.updateItem(updateItemSpec)
    }

    override def deleteItemJSON(tableName: String, keyName: String, key: String, sortKeyName: String = null, sortKeyValue: Number = null): Unit = {
        val table = dynamoDB.getTable(tableName)

        if(sortKeyName != null) {
            val deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(keyName, key, sortKeyName, sortKeyValue)
            table.deleteItem(deleteItemSpec)
        }
        else
            table.deleteItem(keyName, key)

    }

    override def queryJSONItemsByKey(tableName: String, keyName: String, key: String): List[String] = {
        val table = dynamoDB.getTable(tableName)

        val spec = new QuerySpec()
            .withKeyConditionExpression(keyName + " = :v_key")
            .withValueMap(new ValueMap().withString(":v_key", key))
            .withConsistentRead(true)

        val items = table.query(spec)
        items.asScala.map(item => {
            item.toJSON
        }).toList
    }

    override def getAllItemsAsJSON(tableName: String): List[String] = {
        val table = dynamoDB.getTable(tableName)
        val itemCollection = table.scan()
        itemCollection.asScala.map(item => item.toJSON).toList
    }

    override def getPageOfItemsAsJSON(tableName: String, pageNbr: Int, maxPageSize: Int): List[String] = {
        val table = dynamoDB.getTable(tableName)
        val scanSpec = new ScanSpec().withMaxPageSize(maxPageSize).withConsistentRead(true)
        val pagesOfItems = table.scan(scanSpec).pages()
        pagesOfItems.asScala.zipWithIndex.flatMap { case (page, index) =>
            if(index == pageNbr)
                Some(page.asScala.map(item => item.toJSON))
            else
                None
        }.flatten.toList
    }
}

object DynamoDbUtilBuilder {
    private lazy val amazonDynamoDb = AmazonDynamoDBClientBuilder.standard.build

    def build(): NoSQLDbUtility = new DynamoDbUtil(new DynamoDB(amazonDynamoDb), amazonDynamoDb)
}
