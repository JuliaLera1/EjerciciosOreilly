package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ejSanFran {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("Fire")
      .getOrCreate()
//  así  no especificamos schema, dejamos que spark lo infiera de un sample
//    val sampleDF = spark
//      .read
//      .option("samplingRatio", 0.001)
//      .option("header", true)
//      .csv("C:/users/julia.lera/downloads/Fire_incidents.csv")
    //sampleDF.show()
    //val col=sampleDF.columns.length
    //println("EL NUMERO DE COLUMNAS ES: " + col )
//    val fire= spark.sql("SELECT CallNumber, UnitID, IncidentNumber, CallType, CallDate, WatchDate, CallFinalDisposition, AvailableDtTm, Address, City, Zipcode, Battalion," +
//      "StationArea, Box, OriginalPriority, Priority, FinalPriority, ALSUnit, CallTypeGroup, NumAlarms, UnitType, UnitSequenceInCallDispatch, FirePreventionDistrict, SupervisorDistrict" +
//      "Neighborhood, Location, RowID, Delay FROM sampleDF")
    // ahora
//    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true), StructField("UnitID", StringType, true), StructField("IncidentNumber", IntegerType, true),
//    StructField("CallType", StringType, true), StructField("CallDate", StringType, true), StructField("WatchDate", StringType, true),
//    StructField("CallFinalDisposition", StringType, true), StructField("AvailableDtTm", StringType, true), StructField("Address", StringType, true),
//    StructField("City", StringType, true), StructField("Zipcode", IntegerType, true), StructField("Battalion", StringType, true),
//    StructField("StationArea", StringType, true), StructField("Box", StringType, true), StructField("OriginalPriority", StringType, true),
//    StructField("Priority", StringType, true), StructField("FinalPriority", IntegerType, true), StructField("ALSUnit", BooleanType, true), StructField("CallTypeGroup", StringType, true),
//    StructField("Number of Alarms", IntegerType, true), StructField("UnitType", StringType, true), StructField("UnitSequenceInCallDispatch", IntegerType, true),
//    StructField("FirePreventionDistrict", StringType, true), StructField("SupervisorDistrict", StringType, true), StructField("Neighborhood", StringType, true),
//    StructField("Location", StringType, true), StructField("RowID", StringType, true), StructField("Delay", FloatType, true)) )

    val sfFireFile= "C:/users/julia.lera/downloads/Fire_incidents.csv"
    val col = Seq("Call Number","ID","Incident Number", "Incident Date", "Alarm DtTm", "Arrival DtTm", "Close DtTm", "Address", "City", "zipcode", "Battalion",
    "Station Area", "Box", "Primary Situation", "Number of Alarms", "First Unit On Scene", "Action Taken Primary",
      "Supervisor District", "neighborhood_district", "Point")
    val FireDF = spark.read
      .option("header", "true")
      .csv(sfFireFile).select(col.head, col.tail: _*)

    FireDF.show()

    //val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)



  }

}
