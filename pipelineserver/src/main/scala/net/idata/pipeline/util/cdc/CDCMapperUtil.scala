package net.idata.pipeline.util.cdc

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

import net.idata.pipeline.common.model.{CDCAttributes, PipelineEnvironment}
import net.idata.pipeline.common.util.NoSQLDbUtil

object CDCMapperUtil {
    def initialize(datasetName: String, cdcAttributes: CDCAttributes): Unit = {
        deleteEntryIfExists(cdcAttributes)

        // Store the entry
        val key = cdcAttributes.database + "." + cdcAttributes.schema + cdcAttributes.table
        NoSQLDbUtil.setItemNameValue(PipelineEnvironment.values.cdcMapperTableName, "name", key, "value", datasetName)
    }

    def deleteEntryIfExists(cdcAttributes: CDCAttributes): Unit = {
        val key = cdcAttributes.database + "." + cdcAttributes.schema + cdcAttributes.table

        // Make sure an entry already exists for the key
        val existing = NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.cdcMapperTableName, "name", key, "value").orNull
        if(existing != null)
            NoSQLDbUtil.deleteItemJSON(PipelineEnvironment.values.cdcMapperTableName, "name", key)
    }

    def getDatasetName(database: String, schema: String, table: String): String = {
        val key = database + "." + schema + "." + table
        NoSQLDbUtil.getItemJSON(PipelineEnvironment.values.cdcMapperTableName, "name", key, "value").orNull
    }
}
