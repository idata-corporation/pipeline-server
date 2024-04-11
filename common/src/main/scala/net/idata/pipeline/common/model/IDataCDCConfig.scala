package net.idata.pipeline.common.model

case class IDataCDCConfig(
                             enabled: Boolean,
                             `type`: String,
                             databaseSecretName: String,
                             databaseName: String,
                             includeTables: java.util.List[String],
                             pollingInterval: Int,
                             lastReadTableName: String
                         )
