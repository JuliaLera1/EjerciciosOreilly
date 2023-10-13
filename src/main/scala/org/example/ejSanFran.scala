package org.example

//import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


import scala.language.postfixOps


object ejSanFran {
  def Fire(spark: SparkSession) {

    import spark.implicits._ //para poder usar $ como col

//  Así  no especificamos schema, dejamos que spark lo infiera de un sample
//    val sampleDF = spark
//      .read
//      .option("samplingRatio", 0.001)
//      .option("header", true)
//      .csv("C:/users/julia.lera/downloads/Fire_incidents.csv")

    //val col=sampleDF.columns.length
    //println("EL NUMERO DE COLUMNAS ES: " + col

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

    //selección con un where simple
//    val fewFireDF = FireDF
//      .select("Incident Number", "Arrival DtTm", "Primary Situation")
//      .where(col("Primary Situation").startsWith("7"))
//
//    fewFireDF.show()


    val dFire = FireDF.select("Primary Situation").distinct().orderBy(col("Primary Situation"))
     println("La siguiente tabla muestra los distintos tipos de acciones primarias que se toman: \n")
    //dFire.show()


   /* val newFireDF= FireDF.withColumnRenamed("Action Taken Primary", "PrimaryAction")
    //newFireDF.show()

    val fireTsDF = newFireDF.withColumn("IncidentDate", to_timestamp(col("Incident Date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .drop("Incident Date")
      .withColumn("ArrivalTS", to_timestamp(col("Arrival DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Arrival DtTm")
      .withColumn("CloseTS", to_timestamp(col("Close DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Close DtTm")
      .withColumn("AlarmTS", to_timestamp(col("Alarm DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Alarm DtTm")
//importante poner en la segunda parte de to_timestamp en qué formato está nuestra fecha, MM es mes, mm es minuto

*/
    val fireTsDF= bonito(dFire)
//    val resultado = fireTsDF.withColumn("Delay", col("ArrivalTS") - col("IncidentDate"))
    //val resultado = fireTsDF.withColumn("Delay", datediff(col("ArrivalTS"), col("IncidentDate"))) //esto me da 0 siempre, no cuenta las horas creo
    val resultado = fireTsDF.withColumn(
      "Delay",
      (unix_timestamp(col("ArrivalTS")) - unix_timestamp(col("IncidentDate"))) / 3600)
    //esta forma me da las horas
    //resultado.select("neighborhood_district", "Delay").groupBy("neighborhood_district").agg(avg("Delay").as("AvgDelay")).orderBy(col("AvgDelay").desc).show()

//    fireTsDF
//      .select("PrimaryAction")
//     .where(col("PrimaryAction").isNotNull)
//      .groupBy("PrimaryAction")
//      .count()
//      .orderBy(desc("count"))
//      .show(10, false)

//    fireTsDF
//      .select("PrimaryAction")
//      .where(col("PrimaryAction").isNotNull)
//      .distinct()
//      .show(10, false)

    //fireTsDF.select(year(col("IncidentDate"))).distinct().orderBy(year(col("IncidentDate"))).show()

    //fireTsDF.where(month(col("Incidentdate"))===7).agg(count(col("Incident Number"))).show() //si queremos comparar, no vale poner solo un =

    //fireTsDF.groupBy(month(col("Incidentdate"))).agg(countDistinct(col("IncidentDate"))).show()

//    resultado
//      .select(F.sum("Number of Alarms"), F.avg("Delay"),
//        F.min(col("Delay")), F.max("Delay"))
//      .show()
    //Delay.min sale negativo porque hay un fallo en los datos

    //EJERCICIOS EXTRA
    // println("Estos son todas las diferentes situaciones primarias que se dieron durante el año 2018: \n")
    //resultado.select("Primary Situation").where(year(col("IncidentDate"))===2018).groupBy("Primary Situation").count().show()

//    val res2 = resultado.where(year(col("IncidentDate"))===2018).groupBy(month(col("IncidentDate")))//.count().as("ct")
//    println("Aquí se ve qué meses del año 2018 tuvieron mayor cantidad de avisos de fuego: \n")
//    res2.count().orderBy(desc("count")).show()
//
//    println("Esta tabla muestra los vecindarios con más avisos de fuego: \n")
//    resultado.groupBy(col("neighborhood_district")).count().orderBy(desc("count")).show()
//
//    println("Estos son los vecindarios que tuvieron peor tiempo de respuesta en 2018: \n")
//    resultado.where(year(col("IncidentDate"))===2018).groupBy("neighborhood_district").agg(avg("Delay").as("media"))
//      .orderBy(desc("media")).show()

    println("La semana de 2018 con más llamadas fue: \n")
    val resSemana = resultado.withColumn("SemanaAño", weekofyear(col("IncidentDate")))
    resSemana.select("SemanaAño").where(year(col("IncidentDate"))===2018).groupBy(col("SemanaAño")).count()
      .orderBy(desc("count"))
    .show(1)

    resultado.where(col("zipcode").isNotNull).groupBy(col("neighborhood_district"), col("zipcode")).count().orderBy(desc("count")).show()

    println("Podemos ver que los zipcodes de los vecindarios con más llamadas empiezan por 941. Hay 84 zipcode diferentes y 62 empiezan por 941 luego tiene sentido en parte. " +
      "La mayoría de estos zipcodes son de la forma 9410_ o 9411_")

//    val zc=resultado.select("zipcode").distinct().count()
    val zc90 = resultado.select("zipcode").where(col("zipcode").startsWith("9410")).distinct().count()
    val zc91 = resultado.select("zipcode").where(col("zipcode").startsWith("9411")).distinct().count()
    val zc9 = resultado.select("zipcode").where(col("zipcode").startsWith("9411") || col("zipcode").startsWith("9410")).distinct().count()
    println(zc90,zc91)
  }

def bonito(df: DataFrame): DataFrame= {
  val newFireDF = df.withColumnRenamed("Action Taken Primary", "PrimaryAction")
  newFireDF.show()
  /* val fireTsDF = */ newFireDF.withColumn("IncidentDate", to_timestamp(col("Incident Date"), "yyyy-MM-dd'T'HH:mm:ss"))
    .drop("Incident Date")
    .withColumn("ArrivalTS", to_timestamp(col("Arrival DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Arrival DtTm")
    .withColumn("CloseTS", to_timestamp(col("Close DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Close DtTm")
    .withColumn("AlarmTS", to_timestamp(col("Alarm DtTm"), "yyyy-MM-dd'T'HH:mm:ss")).drop("Alarm DtTm")
  //importante poner en la segunda parte de to_timestamp en qué formato está nuestra fecha, MM es mes, mm es minuto
  //fireTsDF //la última línea es lo que devuelve
}

}
