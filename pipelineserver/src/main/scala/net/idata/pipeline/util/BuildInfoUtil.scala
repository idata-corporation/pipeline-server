package net.idata.pipeline.util

object BuildInfoUtil {
    val version: String = "2.3.4"
    private val transformClass = "net.idata.pipeline.transform.Transform"

    def getTransformInfo(environment: String): (String, String) = {
        val file = "s3://" + environment + "-config/spark/" + "pipeline-transform-assembly-" + version + ".jar"
        (file, transformClass)
    }
}