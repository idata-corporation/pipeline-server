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

import com.amazonaws.services.s3.model.{ObjectMetadata, S3Object, S3ObjectSummary}

import java.io.{BufferedReader, ByteArrayInputStream, InputStream}
import java.net.URI

trait ObjectStoreUtility {
    def getBucket(url: String): String

    def getKey(url: String): String

    def getURI(path: String): URI

    def getObjectMetatadata(bucketName: String, key: String): ObjectMetadata

    def readBucketObject(bucketName: String, key: String): Option[String]

    def readBucketObjectFirstRow(bucketName: String, key: String): Option[String]

    def getBufferedReader(bucketName: String, key: String): (BufferedReader, S3Object)

    def getInputStream(bucketName: String, key: String): (InputStream, S3Object)

    def copyBucketObject(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): Unit

    def writeBucketObject(bucketName: String, key: String, content: String): Unit

    def writeBucketObjectFromStream(bucketName: String, key: String, stream: ByteArrayInputStream, metadata: ObjectMetadata): Unit

    def deleteFolder(bucketName: String, key: String): Unit

    def deleteBucketObject(bucketName: String, key: String): Unit

    def listObjects(bucketName: String, key: String): List[String]

    def listSummaries(bucketName: String, key: String): List[S3ObjectSummary]

    def keyExists(bucketName: String, key: String): Boolean
}