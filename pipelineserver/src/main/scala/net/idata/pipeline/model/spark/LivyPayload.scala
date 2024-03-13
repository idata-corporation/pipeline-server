package net.idata.pipeline.model.spark

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

case class LivyPayload(
                          file: String,
                          className: String,
                          args: Array[String],
                          jars: java.util.List[String],
                          conf: java.util.Map[String, String],
                          pyFiles: java.util.List[String],
                          driverMemory: String,
                          executorMemory: String,
                          executorCores: Int,
                          numExecutors: Int
                      )
