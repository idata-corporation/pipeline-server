package net.idata.pipeline.model.aws

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
