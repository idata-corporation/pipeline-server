package net.idata.pipeline.util

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

object BuildInfoUtil {
    val version: String = "2.3.4"
    private val transformClass = "net.idata.pipeline.transform.Transform"

    def getTransformInfo(environment: String): (String, String) = {
        val file = "s3://" + environment + "-config/spark/" + "pipeline-transform-assembly-" + version + ".jar"
        (file, transformClass)
    }
}