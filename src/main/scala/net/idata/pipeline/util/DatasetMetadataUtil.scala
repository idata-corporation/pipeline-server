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

import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.gson.Gson
import net.idata.pipeline.model.{DatasetMetadata, PipelineEnvironment, PipelineException}
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils

import java.io.{BufferedInputStream, ByteArrayInputStream}
import scala.util.control.Breaks._

class DatasetMetadataUtil(statusUtil: StatusUtil) {
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
            statusUtil.info("processing", "Bulk file upload")
            val keys = ObjectStoreUtil.listObjects(ObjectStoreUtil.getBucket(metadata.dataFilePath), ObjectStoreUtil.getKey(metadata.dataFilePath))
            keys.map(key => "s3://" + ObjectStoreUtil.getBucket(metadata.dataFilePath) + "/" + key)
                .filterNot(_.endsWith("/"))
                .filterNot(_.endsWith(".metadata.json"))
        }
        else
            List(metadata.dataFilePath + metadata.dataFileName)
    }

    private def uncompress(bucket: String, key: String): DatasetMetadata = {
        statusUtil.info("processing","Uncompressing bucket: " + bucket + ", key: " + key)

        val (dataset, publisherToken) = parseDatasetPublisherTokenFromKey(key)
        statusUtil.info("processing","Dataset name: " + dataset)

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
                        statusUtil.info("processing","Archive file: " + archiveEntry.getName)
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
        statusUtil.info("processing","Writing archive file to : " + writeDirectory + tempFilename)
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
