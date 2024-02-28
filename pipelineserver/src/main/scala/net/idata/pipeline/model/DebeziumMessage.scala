package net.idata.pipeline.model

case class DebeziumMessage(
                              topic: String,
                              datasetName: String,
                              isInsert: Boolean,
                              isUpdate: Boolean,
                              isDelete: Boolean,
                              before: Map[String, String],
                              after: Map[String, String],
                          )
