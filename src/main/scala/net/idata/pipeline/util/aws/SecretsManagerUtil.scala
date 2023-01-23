package net.idata.pipeline.util.aws

/*
 Copyright 2023 IData Corporation (http://www.idata.net)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import com.google.gson.Gson

object SecretsManagerUtil {
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
