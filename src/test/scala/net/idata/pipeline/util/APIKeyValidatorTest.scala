package net.idata.pipeline.util

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

import net.idata.pipeline.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.util.aws.SecretsManagerUtil
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class APIKeyValidatorTest extends AnyFunSuite {
    PipelineEnvironment.values = new PipelineEnvironment(
        null,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        "my-secret-name",
        null,
        null)

    test("send null key") {
        val thrown = intercept[PipelineException] {
            APIKeyValidator.validate(null)
        }
        assert(thrown.getMessage === "x-api-key does not exist or is invalid")
    }

    test("send invalid key with invalid API keys secret name") {
        val thrown = intercept[PipelineException] {
            val secretsManagerUtil = mock[SecretsManagerUtil]
            when(secretsManagerUtil.getSecretMap("test")).thenReturn(None)
            APIKeyValidator.validate("test")
        }
        assert(thrown.getMessage === "The Secrets Manager entry for value: " + PipelineEnvironment.values.apiKeysSecretName + " was not found")
    }
}
