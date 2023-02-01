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

import net.idata.pipeline.model.PipelineException
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONObject, JSONTokener}
import org.xml.sax.SAXException

import java.io.{ByteArrayInputStream, StringReader}
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

object SchemaValidationUtil {
    def validateJson(data: String, schemaFileUrl: String): Unit = {
        val jsonSchema = ObjectStoreUtil.readBucketObject(ObjectStoreUtil.getBucket(schemaFileUrl), ObjectStoreUtil.getKey(schemaFileUrl))
            .getOrElse(throw new PipelineException("Could not read the validation schema file: " + schemaFileUrl))

        val schemaObject = new JSONObject(new JSONTokener(jsonSchema))
        val dataObject = new JSONObject(new JSONTokener(data))

        val schema = SchemaLoader.load(schemaObject)
        schema.validate(dataObject)
    }

    def validateXml(data: String, schemaFileUrl: String): Unit = {
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
                throw new PipelineException("The XML data did not pass the XML Schema validation against the XML schema: " + schemaFileUrl + ", error: " + e.getMessage)
        }
    }
}
