package net.idata.pipeline.transform.util

import net.idata.pipeline.common.model.DatasetConfig
import org.apache.spark.sql.DataFrame

class IcebergUtil(config: DatasetConfig) extends ParquetUtilBase(config) {
    def write(dfUpdates: DataFrame): Unit = {
        statusUtil.info("processing", "Writing dataset using Iceberg")

        val tablePath = config.source.schemaProperties.dbName + "." + config.name
        dfUpdates.writeTo(tablePath).createOrReplace()

        repairTable()
        /*
        TODO - Do MERGE INTO when the Apache API supports it
        MERGE INTO prod.db.target t -- a target table
        USING (SELECT ...) s        -- the source updates
        ON t.id = s.id              -- condition to find updates for target rows
        WHEN MATCHED AND <predicate_x> THEN DELETE
        WHEN MATCHED AND <predicate_y> THEN UPDATE *
        WHEN NOT MATCHED THEN INSERT *


        MERGE INTO prod.db.table.branch_audit t
        USING (SELECT ...) s
        ON t.id = s.id
        WHEN ...

         */
    }
}
