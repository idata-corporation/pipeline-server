package net.idata.pipeline.transform.util

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

import net.idata.pipeline.common.model.{DatasetConfig, DatasetProperties, PipelineException}
import net.idata.pipeline.common.util.{GuidV5, ObjectStoreUtil}
import net.idata.pipeline.transform.Transform
import org.apache.commons.lang.StringEscapeUtils
import org.apache.poi.ss.usermodel.{CellType, DataFormatter, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.util

class ExcelToCsvUtil(properties: DatasetProperties, config: DatasetConfig) {
    private val sparkSession = Transform.sparkSession
    private val statusUtil = Transform.statusUtil

    def convertExcelToDataFrame(schema: StructType): DataFrame = {
        val csvRows = new util.ArrayList[util.ArrayList[String]]()

        statusUtil.info("processing", "Converting inbound file: " + properties.sourceTransformUrl + " to a CSV format")

        val (inputStream, s3Object) = ObjectStoreUtil.getInputStream(ObjectStoreUtil.getBucket(properties.sourceTransformUrl), ObjectStoreUtil.getKey(properties.sourceTransformUrl))
        val workbook = {
            if(properties.metadata.dataFilePath != null)
                throw new PipelineException("Bulk file ingestion is not supported for Excel files")
            // xlsx file?
            if(properties.metadata.dataFileName.toLowerCase.endsWith(".xlsx"))
                new XSSFWorkbook(inputStream)
            else
                WorkbookFactory.create(inputStream)
        }
        inputStream.close()
        s3Object.close()

        val evaluator = workbook.getCreationHelper.createFormulaEvaluator()
        val formatter = new DataFormatter(true)
        var maxRowWidth = 0

        val sheet = workbook.getSheetAt(config.source.fileAttributes.xlsAttributes.worksheet)
        if(sheet.getPhysicalNumberOfRows == 0)
            throw new PipelineException("The spreadsheet file with sheet " + config.source.fileAttributes.xlsAttributes.worksheet + " does not contain any data: " + properties.sourceTransformUrl)

        for(i <- 0 until (sheet.getLastRowNum + 1)) {
            val row = sheet.getRow(i)
            val csvRow = new util.ArrayList[String]()
            if(row != null) {
                for(n <- 0 until row.getLastCellNum) {
                    val cell = row.getCell(n)
                    if(cell == null)
                        csvRow.add("")
                    else {
                        if(cell.getCellType == CellType.FORMULA)
                            csvRow.add(formatter.formatCellValue(cell, evaluator))
                        else
                            csvRow.add(formatter.formatCellValue(cell))
                    }
                }
                if(row.getLastCellNum > maxRowWidth)
                    maxRowWidth = row.getLastCellNum
            }
            csvRows.add(csvRow)
        }

        val tempFilePath = "s3://" + properties.pipelineEnvironment.environment + "-temp/exceltocsv/" + config.name + "/" + GuidV5.nameUUIDFrom(System.currentTimeMillis().toString).toString + ".csv"
        writeCsvFile(csvRows, maxRowWidth, tempFilePath)

        // Read the temp file back into a dataframe
        if(schema == null) {
            sparkSession
                .read
                .option("header", "true")
                .option("delimiter", getTempFileDelimiter)
                .option("inferSchema", value = true)
                .option("samplingRatio",  value = 1.0)
                .format("csv")
                .load(tempFilePath)
        }
        else {
            sparkSession
                .read
                .option("header", "true")
                .option("delimiter", getTempFileDelimiter)
                .format("csv")
                .schema(schema)
                .load(tempFilePath)
        }

    }

    private def writeCsvFile(rows: util.ArrayList[util.ArrayList[String]], maxRowWidth: Int, tempFilePath: String): Unit = {
        val csv = new StringBuilder()
        val delimiter = getTempFileDelimiter

        // Write out the rows
        rows.forEach(row => {
            var i = 0
            row.forEach(column => {
                csv.append(StringEscapeUtils.escapeCsv(column))
                csv.append(delimiter)
                i = i + 1
            })
            csv.setLength(csv.length - delimiter.length)    // Remove the last delimiter
            while(i < maxRowWidth) {
                csv.append(delimiter)
                i = i + 1
            }
            csv.append("\n")
        })

        statusUtil.info("processing", "Writing temporary CSV file: " + tempFilePath)
        ObjectStoreUtil.writeBucketObject(ObjectStoreUtil.getBucket(tempFilePath), ObjectStoreUtil.getKey(tempFilePath), csv.mkString)
    }

    private def getTempFileDelimiter: String = {
        // Use a pipe if not defined
        if(config.source.fileAttributes.xlsAttributes.tempCsvFileDelimiter == null)
            "|"
        else
            config.source.fileAttributes.xlsAttributes.tempCsvFileDelimiter
    }
}