package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object airports {
  def airpots()(implicit spark: SparkSession): Unit={
    import spark.implicits._

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

    //SELECT BÁSICOS
/*
    print("Esta tabla muestra solo algunos de los datos contenidos en airports_na: \n")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    print("Esta tabla muestra solo algunos de los datos contenidos en departureDelays: \n")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    print("Aquí se muestran todos los datos contenidos en sfo, es decir, los datos sobre vuelos entre Seattle y San Francisco en un periodo de tiempo determinado: \n")
    spark.sql("SELECT * FROM sfo").show()
    print("Observa que tan solo hay tres líneas, en comparación con los más de 3 millones de datos contenidos en departureDelays. \n")
*/

    // UNIONS
/*
    val bar = delays.union(sfo) //unimos la tabla sfo y la tabla delays que claramente tienen el mismo esquema pues la 1º viene de la 2º
    bar.createOrReplaceTempView("bar") //guardamos la union en una TempView
    bar.filter(expr(
      """origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()
*/
    /*
    ahora filtramos para quedarnos con los mimos datos que en sfo, pero repetidos porque los cogemos de ambas tablas (no es un join,
    porque no juntamos a la derecha o a la izquierda si no debajo
     */
/*
    spark.sql(
      """
    SELECT *
     FROM bar
     WHERE origin = 'SEA'
     AND destination = 'SFO'
     AND date LIKE '01010%'
     AND delay > 0
    """).show()
*/

    //JOINS
    print("Ahora, juntamos la tabla sfo con la tabla airports para añadir a los datos de sfo la ciudad y el estado de origen: \n")
    sfo.join(
      airports.as("air"), //unimos la tabla sfo con la de airports, uniendo IATA de airports con la columna ORIGIN de SFO, los datos que no coincidan se eliminan, por tanto, quedan 3 lineas
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination").show() //elegimos las columnas que queremos que se muestren
    //lo mismo en sql:
    spark.sql(
      """
    SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
     FROM foo f
     JOIN airports_na a
     ON a.IATA = f.origin
    """).show()


  }

}
