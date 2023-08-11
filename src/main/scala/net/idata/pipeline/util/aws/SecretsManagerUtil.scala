package net.idata.pipeline.util.aws

/*
IData Pipeline
Copyright (C) 2023 IData Corporation (http://www.idata.net)

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

import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import com.google.gson.Gson

trait SecretsManagerUtil {
    def getSecretMap(secretName: String): Option[java.util.Map[String, String]]
}

object SecretsManagerUtil extends SecretsManagerUtil {
    def getSecretMap(secretName: String): Option[java.util.Map[String, String]] = {
        val result = get(secretName).orNull
        if (result == null)
            None
        else {
            val gson = new Gson
            val mapData = gson.fromJson(result.getSecretString, classOf[java.util.Map[String, String]])
            Some(mapData)
        }
    }

    private def get(secretName: String): Option[GetSecretValueResult] = try {
        val secretsManager = AWSSecretsManagerClientBuilder.defaultClient
        val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName)
        val getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest)
        Option(getSecretValueResult)
    }
    catch {
        case e: Exception => e.printStackTrace()
            None
    }
}
