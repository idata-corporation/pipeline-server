package net.idata.pipeline.util

import net.idata.pipeline.common.model.{PipelineEnvironment, PipelineException}
import net.idata.pipeline.common.util.aws.SecretsManagerUtil
import net.idata.pipeline.model.{RedshiftSecrets, SnowflakeSecrets}

object SecretsUtil {
    def redshiftSecrets(): RedshiftSecrets = {
        val secretName = PipelineEnvironment.values.redshiftSecretName
        val dbSecret = SecretsManagerUtil.getSecretMap(secretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager secret: " + secretName))
        val username = dbSecret.get("username")
        if (username == null)
            throw new PipelineException("Could not retrieve the Redshift username from Secrets Manager secret: " + secretName)
        val password = dbSecret.get("password")
        if (password == null)
            throw new PipelineException("Could not retrieve the Redshift password from Secrets Manager secret: " + secretName)
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if (jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Redshift jdbcUrl from Secrets Manager secret: " + secretName)
        val dbRole = dbSecret.get("dbRole")
        if (dbRole == null)
            throw new PipelineException("Could not retrieve the Redshift dbRole from Secrets Manager secret: " + secretName)

        RedshiftSecrets(
            username,
            password,
            jdbcUrl,
            dbRole,
        )
    }

    def snowflakeSecrets(): SnowflakeSecrets = {
        val dbSecret = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.snowflakeSecretName)
            .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.snowflakeSecretName))
        val username = dbSecret.get("username")
        if (username == null)
            throw new PipelineException("Could not retrieve the Snowflake username from Secrets Manager")
        val password = dbSecret.get("password")
        if (password == null)
            throw new PipelineException("Could not retrieve the Snowflake password from Secrets Manager")
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if (jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Snowflake jdbcUrl from Secrets Manager")
        val stageName = dbSecret.get("stageName")
        if (stageName == null)
            throw new PipelineException("Could not retrieve the Snowflake stageName from Secrets Manager")
        val stageUrl = {
            val url = dbSecret.get("stageUrl")
            if (url == null)
                throw new PipelineException("Could not retrieve the Snowflake stageUrl from Secrets Manager")
            if (url.endsWith("/"))
                url
            else
                url + "/"
        }

        SnowflakeSecrets(
            username,
            password,
            jdbcUrl,
            stageName,
            stageUrl
        )
    }
}
