package com.ExcelReader.murphy.Adequacy

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Cell, DataFormatter, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AdequacyImplementation extends App {

  def addToHive(spark: SparkSession, finalDf: DataFrame): Unit = {
    finalDf.write.format("orc").mode(SaveMode.Append).saveAsTable("osr_uz.tbl_adequacy_ops_procedure")
  }

  def readDataFromExcelSheet(spark: SparkSession, path: String) {
    var adequacy = new Adequacy()
    var numberPlannedForReview = adequacy.NumberPlannedForReview()
    var numberOfProceduresReviewed = adequacy.NumberOfProcedureReviewed()
    var numberOfCarriedForwardProcedure = adequacy.NumberOfCarriedForwardProcedures()
    var percentageOfPlannedVsRev = adequacy.PercentageOfPlannedVsReviewed()
//    var op = adequacy.OP()
    var element = adequacy.Element()

    val setOfMergedRows = mutable.HashMap[Int, Int]()
    val uri = new URI("hdfs://prdmurphy")
    val fs = FileSystem.get(uri,new Configuration())
    val filePath = new Path(path)
    val fsDataInputStream=fs.open(filePath)

    val workbook=new XSSFWorkbook(fsDataInputStream)
//     val workbook=WorkbookFactory.create(new File(path))
    val sheet_user="Form - Document_Q1 2018"
    var sheet_index = Integer.MAX_VALUE
    for (x <- 0 until workbook.getNumberOfSheets) {
      if (workbook.getSheetName(x).trim.toLowerCase == sheet_user.trim.toLowerCase) {
        sheet_index = x
      }
    }

    val df = new DataFormatter()
    val sheet = workbook.getSheetAt(sheet_index)
    val numMergedRegions = sheet.getNumMergedRegions
    for (x <- 0 until numMergedRegions) {
      val firstRow = sheet.getMergedRegion(x).getFirstRow                             //SHEET HARD CODED
      val totalCells = sheet.getMergedRegion(x).getLastRow - firstRow + 1
      if (!setOfMergedRows.contains(firstRow))
        setOfMergedRows.put(firstRow, totalCells)
    }

    val sheetNameSplitter=sheet_user.split(" ")
    val quarterName="Q1"
    val year=2018

    val row_itr = sheet.rowIterator()

    def computeResult(value: String): Int = {
      val data = value.split(" ")
      if (data.size == 1)
        data.head.toInt
      else {
        data(1) match {
          case "+" => data.head.toInt + data.last.toInt
          case "-" => data.head.toInt - data.last.toInt
          case "*" => data.head.toInt * data.last.toInt
        }
      }
    }

    while (row_itr.hasNext) {
      val row = row_itr.next()

      val cell_itr = row.cellIterator()


      while (cell_itr.hasNext) {
        val cell = cell_itr.next()

        if (df.formatCellValue(cell).trim.toLowerCase == "total number of operational procedures") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)

            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              val value = df.formatCellValue(row.getCell(colNum + 21)).trim
              adequacy.totalNoOfOperationalProcedures = computeResult(value)
            }
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "number of outdated ops") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)


            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              val value = df.formatCellValue(row.getCell(colNum + 21)).trim.toInt

              adequacy.noOfOutdatedOp = value
            }
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "last updated op (year)") {
//          println(row.getRowNum)
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)


            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              val value = df.formatCellValue(row.getCell(colNum + 21)).trim

              adequacy.lastUpdatedOpYear = value
            }
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "number planned for review") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)
            var lstTemp = ListBuffer[Int]()
            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              var stringValue = df.formatCellValue(row.getCell(colNum + 21)).trim
              if (stringValue == "")
                stringValue = 0 + ""

              val value = stringValue.toInt
              lstTemp += value
            }
            numberPlannedForReview = adequacy.NumberPlannedForReview(lstTemp.head, lstTemp(1), lstTemp(2), lstTemp(3))
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "number of procedure reviewed") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)
            var lstTemp = ListBuffer[Int]()
            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              var stringValue = df.formatCellValue(row.getCell(colNum + 21)).trim
              if (stringValue == "")
                stringValue = 0 + ""

              val value = stringValue.toInt

              lstTemp += value
            }
            numberOfProceduresReviewed = adequacy.NumberOfProcedureReviewed(lstTemp.head, lstTemp(1), lstTemp(2), lstTemp(3))
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "no of carried forward procedure for review") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)
            var lstTemp = ListBuffer[Int]()
            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              var stringValue = df.formatCellValue(row.getCell(colNum + 21)).trim
              if (stringValue == "")
                stringValue = 0 + ""

              val value = stringValue.toInt
              lstTemp += value
            }
            numberOfCarriedForwardProcedure = adequacy.NumberOfCarriedForwardProcedures(lstTemp.head, lstTemp(1), lstTemp(2), lstTemp(3))
          }
        }
        if (df.formatCellValue(cell).trim.toLowerCase == "% of procedure reviewed vs number planned") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)
            var lstTemp = ListBuffer[Double]()
            if(numberPlannedForReview.quarter1==0 && numberPlannedForReview.quarter2==0 && numberPlannedForReview.quarter3==0 && numberPlannedForReview.quarter4==0){
              lstTemp+=(Double.NaN,Double.NaN,Double.NaN,Double.NaN)
              percentageOfPlannedVsRev = adequacy.PercentageOfPlannedVsReviewed(lstTemp.head,lstTemp(1),lstTemp(2),lstTemp(3))

            }
            else{
            for (x <- rowNum until rowNum + numOfCells) {
//              else{
              try {
              var cellValue = BigDecimal(sheet.getRow(x).getCell(colNum + 21).getStringCellValue).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              if (cellValue < 0)
                cellValue = 0
              lstTemp += cellValue
              }
              catch {
                case ex:Exception => lstTemp+=Double.NaN
              }
            }
              percentageOfPlannedVsRev = adequacy.PercentageOfPlannedVsReviewed(BigDecimal(lstTemp.head * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble, BigDecimal(lstTemp(1) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble, BigDecimal(lstTemp(2) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble, BigDecimal(lstTemp(3) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
            }
          }
        }
//
//        if (df.formatCellValue(cell).trim.toLowerCase == "last updated op (year)") {
//          val rowNum = row.getRowNum
//          val colNum = cell.getColumnIndex
//          if (setOfMergedRows.contains(row.getRowNum)) {
//            val numOfCells = setOfMergedRows(row.getRowNum)
//            for (x <- rowNum until rowNum + numOfCells) {
//              val row = sheet.getRow(x)
//              val InclYear = df.formatCellValue(row.getCell(colNum + 21)).trim.split(" ")
//              op = adequacy.OP(InclYear(0).toInt, InclYear(1).replaceAll("[\\[\\](){}]","").toInt)
//            }
//          }
//        }

        if (df.formatCellValue(cell).trim.toLowerCase == "element") {
          val rowNum = row.getRowNum
          val colNum = cell.getColumnIndex
          if (setOfMergedRows.contains(row.getRowNum)) {
            val numOfCells = setOfMergedRows(row.getRowNum)
            var lstTemp = ListBuffer[String]()
            for (x <- rowNum until rowNum + numOfCells) {
              val row = sheet.getRow(x)
              val value = df.formatCellValue(row.getCell(colNum + 21)).trim
              lstTemp += value
            }
            lstTemp.remove(2)
            element = adequacy.Element(lstTemp.toList)
          }
        }
      }
    }


    var lst = List[Adequacy]()

    def createCollection(): Unit = {
      lst = List(adequacy)
    }


    def createRowRdd(): Seq[Row] = {
      Seq(Row(1, 2018, adequacy.totalNoOfOperationalProcedures, adequacy.lastUpdatedOpYear,adequacy.noOfOutdatedOp,
        numberPlannedForReview.quarter1, numberPlannedForReview.quarter2, numberPlannedForReview.quarter3, numberPlannedForReview.quarter4,
        numberOfCarriedForwardProcedure.quarter1, numberOfCarriedForwardProcedure.quarter2, numberOfCarriedForwardProcedure.quarter3, numberOfCarriedForwardProcedure.quarter4,
        percentageOfPlannedVsRev.quarter1, percentageOfPlannedVsRev.quarter2, percentageOfPlannedVsRev.quarter3, percentageOfPlannedVsRev.quarter4)
      )
    }

    def createSchema(): StructType = {
      StructType(Seq(
        StructField("Document_Id", IntegerType, nullable = false),
        StructField("Year", IntegerType, nullable = false),
        StructField("Op_Procedures", IntegerType, nullable = false),
        StructField("Last_Updated_OP_Year", StringType, nullable = false),
        StructField("Number_Of_outdatedOPs", IntegerType, nullable = false),
        StructField("Planned_for_Review_Q1", IntegerType, nullable = false),
        StructField("Planned_for_Review_Q2", IntegerType, nullable = false),
        StructField("Planned_for_Review_Q3", IntegerType, nullable = false),
        StructField("Planned_for_Review_Q4", IntegerType, nullable = false),
        StructField("Carried_Forward_Procedure_Q1", IntegerType, nullable = false),
        StructField("Carried_Forward_Procedure_Q2", IntegerType, nullable = false),
        StructField("Carried_Forward_Procedure_Q3", IntegerType, nullable = false),
        StructField("Carried_Forward_Procedure_Q4", IntegerType, nullable = false),
        StructField("Perc_of_Procedure_Rvd_Vs__Plnd_Q1", DoubleType, nullable = false),
        StructField("Perc_of_Procedure_Rvd_Vs__Plnd_Q2", DoubleType, nullable = false),
        StructField("Perc_of_Procedure_Rvd_Vs__Plnd_Q3", DoubleType, nullable = false),
        StructField("Perc_of_Procedure_Rvd_Vs__Plnd_Q4", DoubleType, nullable = false)
      ))
    }

    def createDataFrame(): DataFrame = {
      val rowRdd = spark.sparkContext.parallelize(createRowRdd())
      val schema = createSchema()
      spark.createDataFrame(rowRdd, schema)
    }

      val finalDf=createDataFrame()
//          finalDf.show()
          addToHive(spark,finalDf)
//          println("Added to adequacy")
  }
}
