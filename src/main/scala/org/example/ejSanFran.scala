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
      .option("header", "true")
      .csv(sfFireFile).select(columnas.head, columnas.tail: _*)

    //FireDF.show()
    //FireDF.printSchema() //no es el schema que queríamos, pero si le aplico el schema no funciona


    //val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)


    val fewFireDF = FireDF
      .select("Incident Number", "Arrival DtTm", "Primary Situation")
    .where(col("Primary Situation").startsWith("7"))

    //fewFireDF.show()

    val dFire = FireDF.select("Primary Situation").distinct().orderBy(col("Primary Situation"))
     //dFire.show()

    val newFireDF= FireDF.withColumnRenamed("Action Taken Primary", "PrimaryAction")
    //newFireDF.show()

    val fireTsDF = newFireDF.withColumn("IncidentDate", to_timestamp(col("Incident Date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .drop("Incident Date")
      .withColumn("ArrivalTS", to_timestamp(col("Arrival DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Arrival DtTm")
      .withColumn("CloseTS", to_timestamp(col("Close DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Close DtTm")
      .withColumn("AlarmTS", to_timestamp(col("Alarm DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Alarm DtTm")
//importante poner en la segunda parte de to_timestamp en qué formato está nuestra fecha, MM es mes, mm es minuto
    //fireTsDF.select("IncidentDate", "ArrivalTS", "CloseTS", "AlarmTS").show(5,false)

//    val resultado = fireTsDF.withColumn("Delay", col("ArrivalTS") - col("IncidentDate"))
    //val resultado = fireTsDF.withColumn("Delay", datediff(col("ArrivalTS"), col("IncidentDate"))) //esto me da 0 siempre, no cuenta las horas creo
    val resultado = fireTsDF.withColumn(
      "Delay",
      (unix_timestamp(col("ArrivalTS")) - unix_timestamp(col("IncidentDate"))) / 3600)
    //esta forma me da las horas
    //resultado.select("neighborhood_district", "Delay").groupBy("neighborhood_district").agg(avg("Delay").as("AvgDelay")).orderBy(col("AvgDelay").desc).show()

//    fireTsDF
//      .select("PrimaryAction")
//      .where(col("PrimaryAction").isNotNull)
//      .groupBy("PrimaryAction")
//      .count()
//      .orderBy(desc("count"))
//      .show(10, false)
    

  }

}
