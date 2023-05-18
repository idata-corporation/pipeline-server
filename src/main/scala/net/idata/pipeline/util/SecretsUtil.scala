package net.idata.pipeline.util

import com.google.gson.Gson
import net.idata.pipeline.model.{PipelineEnvironment, PipelineException, RedshiftSecrets, SnowflakeSecrets}
import net.idata.pipeline.util.aws.SecretsManagerUtil

object SecretsUtil {
    def getSnowflake: SnowflakeSecrets = {
        if (PipelineEnvironment.values.awsSecrets.enabled) {
            val dbSecret = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.awsSecrets.snowflakeName)
                .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.awsSecrets.snowflakeName))
            decipherSnowflakeMap(dbSecret, "Secrets Manager")
        }
        else if (PipelineEnvironment.values.httpSecrets.enabled) {
            val json = HttpUtil.get(PipelineEnvironment.values.httpSecrets.snowflakeUrl, retry = true)
            val gson = new Gson
            val dbSecret = gson.fromJson(json, classOf[java.util.Map[String, String]])
            decipherSnowflakeMap(dbSecret, PipelineEnvironment.values.httpSecrets.snowflakeUrl)
        }
        else
            throw new PipelineException("The Pipeline environment is configured incorrectly for Secrets.  The AWS secrets and HTTP secrets are both disabled")
    }

    def getRedshift: RedshiftSecrets = {
        if (PipelineEnvironment.values.awsSecrets.enabled) {
            val dbSecret = SecretsManagerUtil.getSecretMap(PipelineEnvironment.values.awsSecrets.redshiftName)
                .getOrElse(throw new PipelineException("Could not retrieve database information from Secrets Manager, secret name: " + PipelineEnvironment.values.awsSecrets.redshiftName))
            decipherRedshiftMap(dbSecret, "Secrets Manager")
        }
        else if (PipelineEnvironment.values.httpSecrets.enabled) {
            val json = HttpUtil.get(PipelineEnvironment.values.httpSecrets.redshiftUrl, retry = true)
            val gson = new Gson
            val dbSecret = gson.fromJson(json, classOf[java.util.Map[String, String]])
            decipherRedshiftMap(dbSecret, PipelineEnvironment.values.httpSecrets.redshiftUrl)
        }
        else
            throw new PipelineException("The Pipeline environment is configured incorrectly for Secrets.  The AWS secrets and HTTP secrets are both disabled")
    }

    private def decipherSnowflakeMap(dbSecret: java.util.Map[String, String], source: String): SnowflakeSecrets = {
        val username = dbSecret.get("username")
        if (username == null)
            throw new PipelineException("Could not retrieve the Snowflake username from " + source)
        val password = dbSecret.get("password")
        if (password == null)
            throw new PipelineException("Could not retrieve the Snowflake password from " + source)
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if (jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Snowflake jdbcUrl from " + source)
        val stageName = dbSecret.get("stageName")
        if (stageName == null)
            throw new PipelineException("Could not retrieve the Snowflake stageName from " + source)
        val stageUrl = {
            val url = dbSecret.get("stageUrl")
            if (url == null)
                throw new PipelineException("Could not retrieve the Snowflake stageUrl from " + source)
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
            stageUrl)
    }

    private def decipherRedshiftMap(dbSecret: java.util.Map[String, String], source: String): RedshiftSecrets = {
        val username = dbSecret.get("username")
        if (username == null)
            throw new PipelineException("Could not retrieve the Redshift username from " + source)
        val password = dbSecret.get("password")
        if (password == null)
            throw new PipelineException("Could not retrieve the Redshift password from " + source)
        val jdbcUrl = dbSecret.get("jdbcUrl")
        if (jdbcUrl == null)
            throw new PipelineException("Could not retrieve the Redshift jdbcUrl from " + source)
        val dbRole = dbSecret.get("dbRole")
        if (dbRole == null)
            throw new PipelineException("Could not retrieve the Redshift dbRole from " + source)

        RedshiftSecrets(
            username,
            password,
            jdbcUrl,
            dbRole)
    }
}