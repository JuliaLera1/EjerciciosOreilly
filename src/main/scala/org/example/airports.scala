package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object airports {
  def airpots()(implicit spark: SparkSession): Unit={

    val delaysPath =
      "src/main/resources/departuredelays.csv"
    val airportsPath =
      "src/main/resources/airport-codes-na.txt"

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")
    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table, it contains only information on three flights originating from Seattle (SEA) to the
    //destination of San Francisco (SFO) for a small time range.
    val sfo = delays.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
     date like '01010%' AND delay > 0"""))
    sfo.createOrReplaceTempView("sfo")

    print("Esta tabla muestra solo algunos de los datos contenidos en airports_na: \n")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    print("Esta tabla muestra solo algunos de los datos contenidos en departureDelays: \n")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    print("Aquí se muestran todos los datos contenidos en sfo, es decir, los datos sobre vuelos entre Seattle y San Francisco en un periodo de tiempo determinado: \n")
    spark.sql("SELECT * FROM sfo").show()
    print("Observa que tan solo hay tres líneas, en comparación con los más de 3 millones de datos contenidos en departureDelays.")




  }

}
