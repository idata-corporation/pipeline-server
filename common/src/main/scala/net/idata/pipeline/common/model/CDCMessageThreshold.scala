package net.idata.pipeline.common.model

// Message threshold values are used to determine when writing to the data store, if faster to write data to
// a file rather than use JDBC directly to store the data
case class CDCMessageThreshold(
                                  objectStore: Int,
                                  redshift: Int,
                                  snowflake: Int
                              )
