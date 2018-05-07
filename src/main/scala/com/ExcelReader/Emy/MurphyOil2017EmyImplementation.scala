//Can't get from any cell where unplanned and metered will be written, they have to be in column 0
// January correspondence to ""
// unplanned correspondence to "%"

package com.ExcelReader.Emy

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{DataFormatter, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

object MurphyOil2017EmyImplementation extends App {

  def addToHive(spark:SparkSession,dataframe:DataFrame,flag:Boolean): Unit = {
    if(flag)
    dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("osr_uz.tbl_overall_equipment_efficiency_oil")
    else
      dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("osr_uz.tbl_overall_equipment_efficiency_gas")

  }


  def readDataFromExcelSheet(spark: SparkSession, path: String, fileType:String) {

//    val uri = new URI("hdfs://sandbox-hdp.hortonworks.com/")
    val uri = new URI("hdfs://prdmurphy")
    val fs = FileSystem.get(uri,new Configuration())
    val filePath = new Path(path)
    val fsDataInputStream=fs.open(filePath)

//    val javaFile= new File(path)   //ADDED
    val wb=new XSSFWorkbook(fsDataInputStream)

//    val wb= WorkbookFactory.create(javaFile)
//    val wb=new XSSFWorkbook(fsDataInputStream)
    var flag=true

    val numOfSheets = wb.getNumberOfSheets

    var tabs=Array[String]()

    if(fileType=="emy")
    tabs = Array("Kikeh", "SNP", "West Patricia", "Serendah", "South Acis", "Patricia", "Permas")

    else if(fileType=="emy2") {
      tabs = Array("Golok Merapuh Serampang Belum")
      flag=false
    }

    for(x <- tabs.indices){
      var result_map = HashMap[String, List[(Int, String, String, Double, Double, Double)]]()
      var sheet_name = tabs(x)
        sheet_name = sheet_name.toLowerCase

        var index = 0

        for (x <- 0 until numOfSheets) {
          if (wb.getSheetName(x).trim.toLowerCase == sheet_name)
            index = x
        }

        //    println("Enter Cell Value for metered :")
        var metered_name = "Total Metered Production"
        metered_name = metered_name.toLowerCase()
        //    println("Enter Cell Value for unplanned :")
        var unplanned_name = "Total Actual Unplanned Deferment (UPD) - WORPE"
        unplanned_name = unplanned_name.toLowerCase()

        val sheet = wb.getSheetAt(index)
        sheet_name = sheet.getSheetName

        val row_itr = sheet.rowIterator()
        val df = new DataFormatter()
        var row_unplanned = 0
        var row_metered = 0

        while (row_itr.hasNext) {
          val row = row_itr.next()
          if (df.formatCellValue(row.getCell(0)).toLowerCase.trim == unplanned_name && df.formatCellValue(row.getCell(1)) != "%") {
            row_unplanned = row.getRowNum
          }
          else if (df.formatCellValue(row.getCell(0)).toLowerCase.trim.matches(metered_name)) {
            row_metered = row.getRowNum
          }
        }

        var weekInd = 0
        var rowWeekInd = 0

        val rowItr1 = sheet.rowIterator()
        val lstMetered = ListBuffer[Double]()
        val lstUnplanned = ListBuffer[Double]()
        while (rowItr1.hasNext) {
          val row = rowItr1.next()
          val cellItr = row.cellIterator()
          while (cellItr.hasNext) {
            val cell = cellItr.next()

            if (df.formatCellValue(cell).trim.toLowerCase.matches("jan") && df.formatCellValue(row.getCell(0)).toLowerCase.trim == "") {
              weekInd = cell.getColumnIndex
              rowWeekInd = row.getRowNum
            }
          }
        }
        val lst_week = List.range(weekInd, weekInd + 12)
        for (x <- lst_week) {
          val cell = sheet.getRow(row_metered).getCell(x)
          var cell_value: Double = Double.NegativeInfinity

          try {
            cell_value = cell.getNumericCellValue
            if (cell_value < 0)
              cell_value = 0
          }
          catch {
            case ex: NullPointerException => ex.printStackTrace()
          }
          cell_value = BigDecimal(cell_value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

          lstMetered += cell_value
        }

        for (x <- lst_week) {
          val cell = sheet.getRow(row_unplanned).getCell(x)
          var cell_value: Double = Double.NegativeInfinity

          try {
            cell_value = cell.getNumericCellValue
            if (cell_value < 0)
              cell_value = 0
          }
          catch {
            case ex: NullPointerException => ex.printStackTrace()
          }
          cell_value = BigDecimal(cell_value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          lstUnplanned += cell_value
        }

        var tempCellIndex = weekInd
        for (x <- lstMetered.indices) {
//          println("x is "+lstMetered(x)+" "+lstUnplanned(x))


          if(lstMetered(x)+lstUnplanned(x)!=0) {
             val result = BigDecimal((lstMetered(x) / (lstMetered(x) + lstUnplanned(x))) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            if (result_map.isEmpty) {
              val weekCell = sheet.getRow(rowWeekInd).getCell(tempCellIndex)
              val weekCellValue = df.formatCellValue(weekCell).trim
              val subRes = (weekCellValue, result)
              val tempSubRes = (2018, sheet_name, weekCellValue, lstUnplanned(x), lstMetered(x), result)
              result_map += (sheet_name -> List(tempSubRes))
              tempCellIndex += 1
            }
            else {
              val lst_subResult = result_map(sheet_name)
              val weekCell = sheet.getRow(rowWeekInd).getCell(tempCellIndex)
              val weekCellValue = df.formatCellValue(weekCell).trim
              val subRes = (weekCellValue, result)
              val tempSubRes = (2018, sheet_name, weekCellValue, lstUnplanned(x), lstMetered(x), result)
              val newList = lst_subResult :+ tempSubRes
              result_map = result_map.updated(sheet_name, newList)
              tempCellIndex += 1
            }
          }

        }
        val res = result_map(sheet_name)


        def createDf(obj: List[(Int, String, String, Double, Double, Double)]): DataFrame = {

          import spark.implicits._

          val frame = spark.sparkContext.parallelize(obj).toDF("Year", "Platform_Name", "Month", "Unplanned_Value", "Metered_Value", "Result")
          frame
        }

        val dataframe=createDf(res)
//          dataframe.show()
//      dataframe.printSchema()
       addToHive(spark,dataframe,flag)
//      println("Added To Hive emy")
    }

  }

}

