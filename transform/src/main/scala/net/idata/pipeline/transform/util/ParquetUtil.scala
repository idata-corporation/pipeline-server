package net.idata.pipeline.transform.util

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

import net.idata.pipeline.common.model._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConverters._

class ParquetUtil(config: DatasetConfig) extends ParquetUtilBase(config) {
    def write(dfUpdates: DataFrame, destinationUrl: String): Unit = {
        statusUtil.info("processing", "Writing dataset to objectStore as parquet")

        val saveMode = {
            if(config.destination.objectStore.sparkWriteMode != null && config.destination.objectStore.sparkWriteMode.compareToIgnoreCase("overwrite") == 0)
                SaveMode.Overwrite
            else
                SaveMode.Append
        }

        // Write out as parquet
        if(objectStore.partitionBy == null || objectStore.partitionBy.size == 0) {
            dfUpdates.write
                .mode(saveMode)
                .parquet(destinationUrl)
        } else {
            dfUpdates.write
                .mode(saveMode)
                .partitionBy(objectStore.partitionBy.asScala:_*)
                .parquet(destinationUrl)
            repairTable()
        }
    }
}