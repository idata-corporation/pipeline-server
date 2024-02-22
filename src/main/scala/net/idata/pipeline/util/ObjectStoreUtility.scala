package net.idata.pipeline.util

/*
IData Pipeline
Copyright (C) 2024 IData Corporation (http://www.idata.net)

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