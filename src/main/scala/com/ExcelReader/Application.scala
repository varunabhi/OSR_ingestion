//SPACES IN FILE NAME
//Adequacy Hard Coded and Its name of quarter and year
//ONLY XLSX CAN BE READ

package com.ExcelReader

import java.io.FileNotFoundException

import com.ExcelReader.Emy.MurphyOil2017EmyImplementation
import com.ExcelReader.aims_Inspection.AimsImplementation
import com.ExcelReader.murphy.Adequacy.AdequacyImplementation
import org.apache.spark.sql.SparkSession

object Application {

  def readData(spark:SparkSession,fileType:String,path:String): Unit = fileType.trim.toLowerCase match {
    case "emy" => MurphyOil2017EmyImplementation.readDataFromExcelSheet(spark,path,fileType)
    case "adequacy" => AdequacyImplementation.readDataFromExcelSheet(spark,path)
    case "aims" => AimsImplementation.readDataFromExcelSheet(spark,path)
    case "emy2" => MurphyOil2017EmyImplementation.readDataFromExcelSheet(spark,path,fileType)
  }


  def getFileTypeAndPath(arg:String): (String,String) ={
    try{
      val arrayOfFileAndPath=arg.split(" ")  //CHANGED FOR TESTING
      val excelType = arrayOfFileAndPath(0)
      val pathOfExcelSheet = arrayOfFileAndPath(1)
      (excelType,pathOfExcelSheet)
    }
    catch {
      case ex:FileNotFoundException => println("Path Provided is Incorrect, Provide The Correct Path"); (null,null)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
        .enableHiveSupport()
      .master("yarn")                                          //enablehivesupport
      .appName("OSR Ingestion").getOrCreate()
    for(x <- 0 until(args.length,2)){
      readData(spark,args(x),args(x+1))
    }

  }
}
