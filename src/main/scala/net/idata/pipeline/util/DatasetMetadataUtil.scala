package net.idata.pipeline.util

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

import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.gson.Gson
import net.idata.pipeline.model.{DatasetMetadata, PipelineEnvironment, PipelineException}
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedInputStream, ByteArrayInputStream}
import scala.util.control.Breaks._

object DatasetMetadataUtil {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def read(bucket: String, key: String): DatasetMetadata = {
        if(key.endsWith(".metadata.json")) {
            // Read the metadata file and create the DatasetMetadata object
            val json = ObjectStoreUtil.readBucketObject(bucket, key).getOrElse(
                throw new PipelineException("Could not read metadata file: " + key + ", from bucket: " + bucket))
            val gson = new Gson
            val metadata = gson.fromJson(json, classOf[DatasetMetadata])
            if (metadata == null)
                throw new PipelineException("Could not parse json metadata in the file: " + key)
            metadata.copy(bulkUpload = true)
        }
        else if(key.toLowerCase.endsWith(".zip") ||
            key.toLowerCase.endsWith(".gz") ||
            key.toLowerCase.endsWith(".tar") ||
            key.toLowerCase.endsWith(".jar"))
        {
            uncompress(bucket, key)
        }
        else {
            // Pull the metadata from the data filename.  [dataset-name].[publisher-token].[whatever].dataset.[csv|json|xml|...]
            try {
                val filename = key.substring(key.lastIndexOf('/') + 1)
                val filepath = "s3://" + bucket + "/" + key.substring(0, key.lastIndexOf('/')) + "/"
                val (dataset, publisherToken) = parseDatasetPublisherTokenFromKey(key)
                DatasetMetadata(dataset,
                    filename,
                    filepath,
                    publisherToken,
                    bulkUpload = false,
                    transformedPath = null)
            } catch {
                case e: Exception =>
                    throw new PipelineException("Could not parse the dataset and/or filename from the bucket key name.  The format required: [dataset-name].[publisher-token].[whatever].dataset.[csv|json|xml|...]")
            }
        }
    }

    def getFiles(metadata: DatasetMetadata): List[String] = {
        // The transformed path will be the latest instance of the files
        if(metadata.transformedPath != null) {
            val keys = ObjectStoreUtil.listObjects(ObjectStoreUtil.getBucket(metadata.transformedPath), ObjectStoreUtil.getKey(metadata.transformedPath))
            keys.map(key => "s3://" + ObjectStoreUtil.getBucket(metadata.transformedPath) + "/" + key)
                .filterNot(_.endsWith("/"))
        }
        else if(metadata.bulkUpload) {
            logger.info("Bulk file upload")
            val keys = ObjectStoreUtil.listObjects(ObjectStoreUtil.getBucket(metadata.dataFilePath), ObjectStoreUtil.getKey(metadata.dataFilePath))
            keys.map(key => "s3://" + ObjectStoreUtil.getBucket(metadata.dataFilePath) + "/" + key)
                .filterNot(_.endsWith("/"))
                .filterNot(_.endsWith(".metadata.json"))
        }
        else
            List(metadata.dataFilePath + metadata.dataFileName)
    }

    private def uncompress(bucket: String, key: String): DatasetMetadata = {
        logger.info("Uncompressing bucket: " + bucket + ", key: " + key)

        val (dataset, publisherToken) = parseDatasetPublisherTokenFromKey(key)
        logger.info("Dataset name: " + dataset)

        val (inputStream, s3Object) = ObjectStoreUtil.getInputStream(bucket, key)
        val tempWriteDirectory = "s3://" + PipelineEnvironment.values.environment + "-raw/temp/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + "/"

        // .gz files extract to only one file
        if(key.toLowerCase.endsWith(".gz")) {
            val bufferedInputStream = new BufferedInputStream(inputStream)
            val gZipInputStream = new GzipCompressorInputStream(bufferedInputStream)
            val byteArray = IOUtils.toByteArray(gZipInputStream)

            writeArchivedFile(byteArray, tempWriteDirectory)

            bufferedInputStream.close()
            gZipInputStream.close()
        }
        else {
            val archiveInputStream = {
                if(key.toLowerCase.endsWith(".zip"))
                    new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP, inputStream)
                else if(key.toLowerCase.endsWith(".tar"))
                    new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR, inputStream)
                else if(key.toLowerCase.endsWith(".jar"))
                    new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.JAR, inputStream)
                else
                    throw new PipelineException("Archive type for key: " + key + " is not supported")
            }

            // Write out all of the files in the archive to the temp directory
            breakable {
                while(true) {
                    val archiveEntry = archiveInputStream.getNextEntry
                    if(archiveEntry == null)
                        break

                    // Ignore directories and junk entries for compressed files
                    if(! archiveEntry.isDirectory &&
                        ! archiveEntry.getName.startsWith("__MAC") &&
                        ! archiveEntry.getName.startsWith("META-INF") &&
                        ! archiveEntry.getName.startsWith("./._"))
                    {
                        logger.info("Archive file: " + archiveEntry.getName)
                        val byteArray = IOUtils.toByteArray(archiveInputStream)
                        writeArchivedFile(byteArray, tempWriteDirectory)
                    }
                }
            }
        }

        inputStream.close()
        s3Object.close()

        DatasetMetadata(
            dataset,
            null,
            tempWriteDirectory,
            publisherToken,
            bulkUpload = true,
            transformedPath = null)
    }

    private def writeArchivedFile(byteArray: Array[Byte], writeDirectory: String): Unit = {
        val byteArrayInputStream = new ByteArrayInputStream(byteArray)
        val metadata = new ObjectMetadata()
        metadata.setContentLength(byteArray.length)

        // Write the temp file name
        val tempFilename = GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".tmp"
        logger.info("Writing archive file to : " + writeDirectory + tempFilename)
        ObjectStoreUtil.writeBucketObjectFromStream(
            ObjectStoreUtil.getBucket(writeDirectory),
            ObjectStoreUtil.getKey(writeDirectory + tempFilename),
            byteArrayInputStream,
            metadata
        )
        byteArrayInputStream.close()
    }

    private def parseDatasetPublisherTokenFromKey(key: String): (String, String) = {
        // Pull the metadata from the data filename.  [dataset-name].[publisher-token].[whatever].dataset.[csv|json|xml|...]
        val filename = key.substring(key.lastIndexOf('/') + 1)
        val tokens = filename.split("\\.")
        val dataset = tokens(0)

        val publisherToken = {
            if (GuidV5.isValidUUID(tokens(1)))
                tokens(1)
            else
                GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString
        }
        (dataset, publisherToken)
    }
}
