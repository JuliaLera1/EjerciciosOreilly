package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.language.postfixOps
object Cap4 {
  def main(args: Array[String]) {

    //empezamos la sesión de Spark
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Cap4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val csvFile="src/main/resources/departuredelays.csv"
    val sch = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      //.option("inferSchema", "true")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .load(csvFile)

    // es el mismo df
//    val df3 = spark.read
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .csv("src/main/resources/departuredelays.csv")

    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")
    //PRIMEROS EJERCICIOS
    /*
    df.select("distance", "origin", "destination").where(col("distance").gt(1000)).orderBy(desc("distance")).show()
    val df1=df.select("*").where(col("origin")==="SFO" && col("destination")==="ORD")
    df1.where(col("delay").gt(120)).orderBy(desc("delay")).show()

     */
/*
* It seems there were many significantly delayed flights between these two cities, on dif‐
ferent dates. (As an exercise, convert the date column into a readable format and find
the days or months when these delays were most common. Were the delays related to
winter months or holidays?)
*/
    val dfDates= df.withColumn("dateTS", to_timestamp(col("date"), "MMddHHmm")).drop("date")
     //pone el año 1970, pero no es muy importante para los ejercicios
    /*
     val dfDates1 = dfDates.select("*").where(col("origin") === "SFO" && col("destination") === "ORD")
    dfDates1.where(col("delay").gt(120)).groupBy(month(col("dateTS"))).count().show()
    dfDates1.where(col("delay").gt(120)).groupBy(dayofmonth(col("dateTS"))).count().orderBy(desc("count")).show()
*/
    val dfDatesDelay = dfDates.withColumn("TypeDelay",
      when(col("delay") > 360, "Very Long Delay")
        .when(col("delay") > 120 && col("delay") <= 360, "Long Delay")
        .when(col("delay") > 60 && col("delay") <= 120, "Short Delay")
        .when(col("delay") > 0 && col("delay") <= 60, "Tolerable Delay")
        .when(col("delay") === 0, "No Delay")
        .otherwise("Early"))

    //¿CÓMO PUEDO HACERLO USANDO CASE COMO LO HACE EN EL LIBRO?
    val dfconteo= dfDatesDelay.groupBy(col("TypeDelay")).agg(count("*").alias("conteo"))
    val totalFlights=dfDatesDelay.count()
    val dfPercent= dfconteo.withColumn("percentage", (col("conteo")/totalFlights)*100)
    dfPercent.show()

    spark.catalog.listDatabases()
    spark.catalog.listTables()
    spark.catalog.listColumns("us_delay_flights_tbl")//.show()

    /*
    * Let’s assume you have an existing database, learn_spark_db, and table,
    us_delay_flights_tbl, ready for use. Instead of reading from an external JSON file,
    you can simply use SQL to query the table and assign the returned result to a
    DataFrame:
*/

    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")
//Now you have a cleansed DataFrame read from an existing Spark SQL table

  }

}
