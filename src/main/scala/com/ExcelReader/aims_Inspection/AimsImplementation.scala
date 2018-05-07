//Empty Last Inspection and Nil values
//Deal with Empty Values

package com.ExcelReader.aims_Inspection

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame,  SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap

object AimsImplementation extends App {

  def addToHive(spark: SparkSession, finalDataFRame: DataFrame): Unit = {
    finalDataFRame.write.format("orc").mode(SaveMode.Append).saveAsTable("osr_uz.tbl_aims_inspection_program")
  }

  def readDataFromExcelSheet(spark: SparkSession, path: String) {
    var mapOfMergedRegions: TreeMap[Int, (Int, Int)] = TreeMap[Int, (Int, Int)]()
    val uri = new URI("hdfs://prdmurphy")
    val fs = FileSystem.get(uri,new Configuration())
    val filePath = new Path(path)
    val fsDataInputStream=fs.open(filePath)
    val workbook=new XSSFWorkbook(fsDataInputStream)
    val reader = new DataFormatter()


    var firstQuarterStartIndex: Int = 49
    val firstQuarterEndIndex = firstQuarterStartIndex + 11
    val secondQuarterStartIndex = firstQuarterEndIndex + 1
    val secondQuarterEndIndex = secondQuarterStartIndex + 11
    val thirdQuarterStartIndex = secondQuarterEndIndex + 1
    val thirdQuarterEndIndex = thirdQuarterStartIndex + 11
    val fourthQuarterStartIndex = thirdQuarterEndIndex + 1
    val fourthQuarterEndIndex = fourthQuarterStartIndex + 11

    def getRowAndColumnIndex(sheet: Sheet, cellValue: String): (Int, Int) = {
      val rowIterator = sheet.rowIterator()
      var cellIndex = (-1, -1)
      while (rowIterator.hasNext) {
        val currentRow = rowIterator.next()
        val cellIterator = currentRow.cellIterator()

        while (cellIterator.hasNext) {
          val currentCell = cellIterator.next()
          if (reader.formatCellValue(currentCell).trim.toLowerCase == cellValue.trim.toLowerCase)
            cellIndex = (currentRow.getRowNum, currentCell.getColumnIndex)
        }
      }
      cellIndex
    }

    def findPercPlndVsCompleted(rowNumber: Int, sheet: Sheet): (Double, Double, Double, Double) = {

      val plannedRow = sheet.getRow(rowNumber)
      val completedRow = sheet.getRow(rowNumber + 1)
      var countPlanned: Double = 0
      var countCompleted: Double = 0
      for (x <- firstQuarterStartIndex to firstQuarterEndIndex) {
        if (plannedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countPlanned += 1
        }
        if (completedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countCompleted += 1
        }
      }

      var resultQ1: Double = -1

      if (countPlanned == 0) {
        if (countCompleted == 0)
          resultQ1 = Double.NaN
        else
          resultQ1 = 0
      }
      else {
        resultQ1 = (countCompleted / countPlanned) * 100
      }

      countPlanned = 0
      countCompleted = 0


      for (x <- secondQuarterStartIndex to secondQuarterEndIndex) {
        if (plannedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countPlanned += 1
        }
        if (completedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countCompleted += 1
        }
      }

      var resultQ2: Double = -1

      if (countPlanned == 0) {
        if (countCompleted == 0)
          resultQ2 = Double.NaN
        else
          resultQ2 = 0
      }
      else {
        resultQ2 = (countCompleted / countPlanned) * 100
      }

      countPlanned = 0
      countCompleted = 0


      for (x <- thirdQuarterStartIndex to thirdQuarterEndIndex) {
        if (plannedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countPlanned += 1
        }
        if (completedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countCompleted += 1
        }
      }

      var resultQ3: Double = -1

      if (countPlanned == 0) {
        if (countCompleted == 0)
          resultQ3 = Double.NaN
        else
          resultQ3 = 0
      }
      else {
        resultQ3 = (countCompleted / countPlanned) * 100
      }

      countPlanned = 0
      countCompleted = 0


      for (x <- fourthQuarterStartIndex to fourthQuarterEndIndex) {
        if (plannedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countPlanned += 1
        }
        if (completedRow.getCell(x).getCellType == Cell.CELL_TYPE_NUMERIC) {
          countCompleted += 1
        }
      }

      var resultQ4: Double = -1

      if (countPlanned == 0) {
        if (countCompleted == 0)
          resultQ4 = Double.NaN
        else
          resultQ4 = 0
      }
      else {
        resultQ4 = (countCompleted / countPlanned) * 100
      }

      (resultQ1, resultQ2, resultQ3, resultQ4)
    }

    def addDetailsToLocation(sheet: Sheet, locationDetails: (Int, Int), locationColumn: Int): Location = {
      val startingRow = locationDetails._1
      val lastRow = locationDetails._2
      val firstRow = sheet.getRow(startingRow)
      val firstLocationCell = firstRow.getCell(locationColumn)
      val locationName = reader.formatCellValue(firstLocationCell)
      val location = new Location(locationName)
      val pltfInsallCol = locationColumn + 3
      location.platformInstallation = reader.formatCellValue(firstRow.getCell(pltfInsallCol))

      for (x <- startingRow to(lastRow, 2)) {
        val descCol = locationColumn + 1
        val freqCol = locationColumn + 2
        val lastInspectionCol = locationColumn + 4
        val descValue = reader.formatCellValue(sheet.getRow(x).getCell(descCol))
        val surveyFreqValue = reader.formatCellValue(sheet.getRow(x).getCell(freqCol))
        val lastInspValue = reader.formatCellValue(sheet.getRow(x).getCell(lastInspectionCol))

        val (quarter1, quarter2, quarter3, quarter4) = findPercPlndVsCompleted(x, sheet)
        location.descriptionMap += (descValue -> (surveyFreqValue, lastInspValue, quarter1, quarter2, quarter3, quarter4))
      }
      location
    }

    def addMergedRegionsAddressToMap(sheet: Sheet, columnIndexOfLocation: Int): Unit = {
      var listOfMergedRegions = sheet.getMergedRegions.toList
      listOfMergedRegions = listOfMergedRegions.filter(_.getFirstColumn == columnIndexOfLocation)
      for (x <- listOfMergedRegions.indices) {
        val firstRow = listOfMergedRegions(x).getFirstRow
        val lastRow = listOfMergedRegions(x).getLastRow
        mapOfMergedRegions += (x -> (firstRow, lastRow))
      }
    }

    val NumOfSheets = workbook.getNumberOfSheets

    for (x <- 0 until NumOfSheets) {
      val sheet = workbook.getSheetAt(x)
      val (row, column) = getRowAndColumnIndex(sheet, "Location")
      val columnIndexOfLocation = column
      addMergedRegionsAddressToMap(sheet, columnIndexOfLocation)
      val (monthRow, monthStartCol) = getRowAndColumnIndex(sheet, "January 2018")
      firstQuarterStartIndex = monthStartCol
      val bunchOfLoc = mapOfMergedRegions.map(x => addDetailsToLocation(sheet, x._2, columnIndexOfLocation))
      val locationIterator = bunchOfLoc.iterator

      val locationList = DataFrameCreator.getListOfLocations(locationIterator)
      val spark = SparkSession.builder().master("local[*]").appName("AIMS_Inspaection").config("spark.sql.warehouse.dir", "C:\\tmp\\warehouse").getOrCreate()


      val dataFRame = DataFrameCreator.createDataFrame(spark, locationList)
      val finalDataFRame=dataFRame.filter(dataFRame.col("Location").=!=("Location"))
//      finalDataFRame.show(300)
      addToHive(spark,finalDataFRame)
//      println("Added to hive aims")
    }

  }
}
