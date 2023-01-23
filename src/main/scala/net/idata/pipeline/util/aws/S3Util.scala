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

import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import net.idata.pipeline.util.ObjectStoreUtility

import java.io.{BufferedReader, ByteArrayInputStream, InputStream, InputStreamReader}
import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class S3Utility(val s3: AmazonS3) extends ObjectStoreUtility {
    override def getBucket(url: String): String = {
        val uri = new AmazonS3URI(url)
        uri.getBucket
    }

    override def getKey(url: String): String = {
        val uri = new AmazonS3URI(url)
        uri.getKey
    }

    override def getURI(path: String): URI = {
        val uri = new AmazonS3URI(path)
        s3.getUrl(uri.getBucket, uri.getKey).toURI
    }

    override def getObjectMetatadata(bucketName: String, key: String): ObjectMetadata = {
        s3.getObjectMetadata(bucketName, key)
    }

    override def readBucketObject(bucketName: String, key: String): Option[String] = {
        val (reader, s3Object) = getBufferedReader(bucketName, key)
        val data = Some(Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n"))
        reader.close()
        s3Object.close()
        data
    }

    override def readBucketObjectFirstRow(bucketName: String, key: String): Option[String] = {
        val (reader, s3Object) = getBufferedReader(bucketName, key)
        val firstRow = Some(reader.readLine())
        reader.close()
        s3Object.getObjectContent.abort()
        s3Object.close()
        firstRow
    }

    override def getBufferedReader(bucketName: String, key: String): (BufferedReader, S3Object) = {
        val s3object = s3.getObject(new GetObjectRequest(bucketName, key))
        val contentType = s3object.getObjectMetadata.getContentType // TODO: Use this later to make sure it is a text file
        (new BufferedReader(new InputStreamReader(s3object.getObjectContent)), s3object)
    }

    override def getInputStream(bucketName: String, key: String): (InputStream, S3Object) = {
        val s3object = s3.getObject(new GetObjectRequest(bucketName, key))
        (s3object.getObjectContent, s3object)
    }

    override def copyBucketObject(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): Unit = {
        val copyObjRequest = new CopyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey)
        s3.copyObject(copyObjRequest)
    }

    override def writeBucketObject(bucketName: String, key: String, content: String): Unit = {
        s3.putObject(bucketName, key, content)
    }

    override def writeBucketObjectFromStream(bucketName: String, key: String, stream: ByteArrayInputStream, metadata: ObjectMetadata): Unit = {
        s3.putObject(bucketName, key, stream, metadata)
    }

    override def deleteFolder(bucketName: String, key: String): Unit = {
        if(keyExists(bucketName, key)) {
            val summaries = listSummaries(bucketName, key)
            if(summaries != null) {
                val keys = summaries.map(_.getKey)
                val deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(keys:_*)
                s3.deleteObjects(deleteObjectsRequest)
            }
        }
    }

    override def deleteBucketObject(bucketName: String, key: String): Unit = {
        s3.deleteObject(new DeleteObjectRequest(bucketName, key))
    }

    override def listObjects(bucketName: String, key: String): List[String] = {
        val listOfObjects = new ListBuffer[String]

        breakable {
            val listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key)
            while (true) {
                val listObjects = s3.listObjectsV2(listObjectsRequest)
                val summaries = listObjects.getObjectSummaries
                val keyList = summaries.asScala.map(summary => summary.getKey).toList
                listOfObjects ++= keyList

                if(listObjects.isTruncated) {
                    val continuationToken = listObjects.getNextContinuationToken
                    if(continuationToken != null)
                        listObjectsRequest.setContinuationToken(continuationToken)
                }
                else
                    break
            }
        }

        listOfObjects.toList
    }

    override def listSummaries(bucketName: String, key: String): List[S3ObjectSummary] = {
        val listOfSummaries = new ListBuffer[S3ObjectSummary]

        breakable {
            val listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key)
            while (true) {
                val listObjects = s3.listObjectsV2(listObjectsRequest)
                val summaries = listObjects.getObjectSummaries
                listOfSummaries ++= summaries.asScala

                if(listObjects.isTruncated) {
                    val continuationToken = listObjects.getNextContinuationToken
                    if(continuationToken != null)
                        listObjectsRequest.setContinuationToken(continuationToken)
                }
                else
                    break
            }
        }

        listOfSummaries.toList
    }

    override def keyExists(bucketName: String, key: String): Boolean = {
        val listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key)
        val listObjects = s3.listObjectsV2(listObjectsRequest)
        listObjects.getObjectSummaries.size() > 0
    }
}

object S3UtilBuilder {
    private lazy val amazonS3Client = AmazonS3ClientBuilder.standard.build

    def build(): ObjectStoreUtility = new S3Utility(amazonS3Client)
}
