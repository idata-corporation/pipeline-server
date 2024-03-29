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

import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.aws.SecretsManagerUtil

import scala.collection.JavaConverters._

object APIKeyValidator {
    def validate(apiKey: String): Unit = {
        if(PipelineEnvironment.values.useApiKeys) {
            if(apiKey == null)
                throw new PipelineException("x-api-key does not exist or is invalid")

            val apiKeysMap = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.apiKeysSecretName)
                .getOrElse(throw new PipelineException("The Secrets Manager entry for value: " + PipelineEnvironment.values.apiKeysSecretName + " was not found"))
            val apiKeys = apiKeysMap.asScala.map { case (key, value) => value }.toList

            if(! apiKeys.contains(apiKey))
                throw new PipelineException("Invalid x-api-key: " + apiKey)
        }
    }
}
