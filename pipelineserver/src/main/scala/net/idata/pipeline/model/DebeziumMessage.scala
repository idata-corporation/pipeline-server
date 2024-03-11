package net.idata.pipeline.model

case class DebeziumMessage(
                              topic: String,
                              datasetName: String,
                              isInsert: Boolean,
                              isUpdate: Boolean,
                              isDelete: Boolean,
                              before: java.util.Map[String, String],
                              after: java.util.Map[String, String],
                          )
