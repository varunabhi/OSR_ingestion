//Spark Sessions Different or same

package com.ExcelReader.aims_Inspection

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameCreator {

  def getListOfLocations(locations:Iterator[Location]): List[(String,String,String,String,String,Double,Double,Double,Double)] ={
    var locationToList=List[(String,String,String,String,String,Double,Double,Double,Double)]()
    while (locations.hasNext) {
      val currLocation = locations.next()
      val numOfDesc = currLocation.descriptionMap.size
      val descMap = currLocation.descriptionMap
      val locName = currLocation.getLocationName
      val pltfInst = currLocation.platformInstallation
      val descNameIter = descMap.keysIterator
      while (descNameIter.hasNext) {
        val desc = descNameIter.next()
        //            println((locName,desc,descMap(desc)._1,pltfInst,descMap(desc)._2))
        locationToList = locationToList :+ (locName, desc, descMap(desc)._1, pltfInst, descMap(desc)._2,descMap(desc)._3,descMap(desc)._4,descMap(desc)._5,descMap(desc)._6)
      }
    }
    locationToList
    }


  def createDataFrame(spark:SparkSession,listOfLocations:List[(String,String,String,String,String,Double,Double,Double,Double)]): DataFrame ={

    import spark.implicits._

        val rddLocations=spark.sparkContext.parallelize(listOfLocations)
            val dataFrameLocations=rddLocations.toDF("Location","Description","Survey_Frequency","Platform_Installation","Last_Inspection","Quarter1","Quarter2","Quarter3","Quarter4")
            dataFrameLocations
  }
  }

