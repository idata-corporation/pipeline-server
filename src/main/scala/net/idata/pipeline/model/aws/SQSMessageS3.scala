package net.idata.pipeline.model.aws

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

case class UserIdentity(
                           principalId: String
                       )
case class RequestParameters(
                                sourceIPAddress: String
                            )
case class ResponseElements(
                               `x-amz-request-id`: String,
                               `x-amz-id-2`: String
                           )
case class Bucket(
                     name: String,
                     ownerIdentity: UserIdentity,
                     arn: String
                 )
case class ObjectBis(
                        key: String,
                        size: Double,
                        eTag: String,
                        sequencer: String
                    )
case class S3(
                 s3SchemaVersion: String,
                 configurationId: String,
                 bucket: Bucket,
                 `object`: ObjectBis
             )
case class Records(
                      eventVersion: String,
                      eventSource: String,
                      awsRegion: String,
                      eventTime: String,
                      eventName: String,
                      userIdentity: UserIdentity,
                      requestParameters: RequestParameters,
                      responseElements: ResponseElements,
                      s3: S3
                  )
case class SQSMessageS3(
                             Records: java.util.List[Records]
                         )
