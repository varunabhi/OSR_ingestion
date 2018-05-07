package com.ExcelReader.aims_Inspection

import scala.collection.mutable

class Location(locationName:String) {
  val getLocationName = locationName
  var descriptionMap= mutable.LinkedHashMap[String,(String,String,Double,Double,Double,Double)]()
  var platformInstallation:String=""
  var quarter1Percentage:Int = -1
  var quarter2Percentage:Int = -1
  var quarter3Percentage:Int = -1
  var quarter4Percentage:Int = -1
}
