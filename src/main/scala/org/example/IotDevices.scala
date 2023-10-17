package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object IotDevices {
  def arcjson(spark: SparkSession){
    import spark.implicits._
    /* case class DeviceIoTData(battery_level: Long, c02_level: Long,
                             cca2: String, cca3: String, cn: String, device_id: Long,
                             device_name: String, humidity: Long, ip: String, latitude: Double,
                             lcd: String, longitude: Double, scale: String, temp: Long,
                             timestamp: Long)

     */
    val IoT = spark.read
      .json("src/main/resources/iot_devices.json")
      //.as[DeviceIoTData]


    /*  val filterTempDS = IoT.filter(col("temp").gt(30) && col("humidity").gt(70))
       // filterTempDS.show(5, false)


    val dsTemp = IoT.filter($"temp"> 25)
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .toDF("temp", "device_name", "device_id", "cca3")

    println(dsTemp.first())
    */
    //cómo uso map? por qué no puedo hacer como en el libro d=>d.temp>25
//EJERCICIOS EXTRA END_TO_END
    /*
    IoT.where(col("battery_level").lt(2)).show()
    val ds=IoT.select("cn").where(col("c02_level").gt(1500))
    println("Estos son los países que alguna vez superan el nivel 1500 de CO2: \n")
    ds.groupBy("cn").count().show()
    val MinMax = IoT.agg(max(col("temp")).alias("max_temp"), min("temp").alias("min_temp"),
      max("battery_level").alias("max_battery"), min("battery_level").alias("min_battery"),
      max("c02_level").alias("max_c02"), min("c02_level").alias("min_c02"),
      max("humidity").alias("max_humidity"), min("humidity").alias("min_humidity"))
    println("Estos son los valores máximos y mínimos de ciertos campos: ")
    MinMax.show()
    val minMaxRows = IoT.join(MinMax, Seq())
    //De esta forma podemos obtener qué entradas dan con esos valores máximos y mínimos
    minMaxRows.filter(col("temp")===col("max_temp")).show()
    */
    IoT.groupBy("cn").agg(avg("c02_level").alias("c02_avg")).orderBy(desc("c02_avg")).show()



  }

}
