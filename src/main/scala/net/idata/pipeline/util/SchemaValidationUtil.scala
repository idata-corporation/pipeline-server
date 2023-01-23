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

import net.idata.pipeline.model.PipelineException
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONObject, JSONTokener}
import org.xml.sax.SAXException

import java.io.{ByteArrayInputStream, StringReader}
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

object SchemaValidationUtil {
    def validateJson(dataFileUrl: String, schemaFileUrl: String): Unit = {
        val data = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(dataFileUrl), ObjectStoreUtil.getKey(dataFileUrl))
            .getOrElse("Could not read the data file: " + dataFileUrl)

        val jsonSchema = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(schemaFileUrl), ObjectStoreUtil.getKey(schemaFileUrl))
            .getOrElse(throw new PipelineException("Could not read the validation schema file: " + schemaFileUrl))

        val schemaObject = new JSONObject(new JSONTokener(jsonSchema))
        val dataObject = new JSONObject(new JSONTokener(data))

        val schema = SchemaLoader.load(schemaObject)
        schema.validate(dataObject)
    }

    def validateXml(dataFileUrl: String, schemaFileUrl: String): Unit = {
        val data = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(dataFileUrl), ObjectStoreUtil.getKey(dataFileUrl))
            .getOrElse("Could not read the data file: " + dataFileUrl)

        val xmlSchema = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(schemaFileUrl), ObjectStoreUtil.getKey(schemaFileUrl))
            .getOrElse(throw new PipelineException("Could not read the validation schema: " + schemaFileUrl))
        val xmlDataStream = new ByteArrayInputStream(data.getBytes())

        val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        try {
            val schema = schemaFactory.newSchema(new StreamSource(new StringReader(xmlSchema)))
            val validator = schema.newValidator()
            validator.validate(new StreamSource(xmlDataStream))
        }
        catch {
            case e: SAXException =>
                throw new PipelineException("The XML file: " + dataFileUrl + " did not pass the XML Schema validation against the XML schema: " + schemaFileUrl + ", error: " + e.getMessage)
        }
    }
}
