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


    val fireSchema = StructType(Array(StructField("Call Number", IntegerType, true), StructField("ID", StringType, true), StructField("Incident Number", IntegerType, true),
    StructField("Incident Date", StringType, true), StructField("Alarm DtTm", StringType, true),
    StructField("Arrival DtTm", StringType, true), StructField("Close DtTm", StringType, true), StructField("Address", StringType, true),
    StructField("City", StringType, true), StructField("zipcode", IntegerType, true), StructField("Battalion", StringType, true),
    StructField("Station Area", StringType, true), StructField("Box", StringType, true), StructField("Primary Situation", StringType, true),
    StructField("Number of Alarms", IntegerType, true), StructField("First Unit On Scene", StringType, true),
    StructField("Action Taken Primary", StringType, true), StructField("Supervisor District", StringType, true), StructField("neighborhood_district", StringType, true),
    StructField("Point", StringType, true)) )

    val sfFireFile= "C:/users/julia.lera/downloads/Fire_incidents.csv"
    val columnas = Seq("Call Number","ID","Incident Number","Incident Date", "Alarm DtTm", "Arrival DtTm", "Close DtTm", "Address", "City", "zipcode", "Battalion",
    "Station Area", "Box", "Primary Situation", "Number of Alarms", "First Unit On Scene", "Action Taken Primary",
      "Supervisor District", "neighborhood_district", "Point")
    //no puedes darle el nombre col porque col es una función propia de Spark
    val FireDF = spark.read
      //.schema(fireSchema) //por alguna razón si pongo así me desorena las columnas
      .option("header", "true")
      .csv(sfFireFile).select(columnas.head, columnas.tail: _*)


    //FireDF.show()

    //val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)

    //FireDF.printSchema() //no es el schema que queríamos

    val fewFireDF = FireDF
      .select("Incident Number", "Arrival DtTm", "Primary Situation")
      .where(col("Primary Situation").startsWith("7"))

    //fewFireDF.show()

    val dFire = FireDF.select("Primary Situation").distinct().orderBy(col("Primary Situation"))
     dFire.show()





  }

}
