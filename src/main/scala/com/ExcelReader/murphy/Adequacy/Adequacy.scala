package com.ExcelReader.murphy.Adequacy

class Adequacy {
  var totalNoOfOperationalProcedures:Int=Integer.MAX_VALUE
  var noOfOutdatedOp:Int=Integer.MAX_VALUE
  var lastUpdatedOpYear:String=""
  case class Element(var list: List[String]=List()) {}

//  case class OP(var lastUpdatedYear: String=""

  case class NumberPlannedForReview(var quarter1: Int=0, var quarter2: Int=0, var quarter3: Int=0, var quarter4: Int=0)

  case class NumberOfProcedureReviewed(var quarter1: Int=0, var quarter2: Int=0, var quarter3: Int=0, var quarter4: Int=0)

  case class NumberOfCarriedForwardProcedures(var quarter1: Int=0, var quarter2: Int=0, var quarter3: Int=0, var quarter4: Int=0)

  case class PercentageOfPlannedVsReviewed(var quarter1: Double=Double.NaN, var quarter2: Double=Double.NaN, var quarter3: Double=Double.NaN, var quarter4: Double=Double.NaN)

}
