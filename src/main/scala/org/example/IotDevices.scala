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
    //IoT.where(col("battery_level").lt(2)).show()
    val ds=IoT.select("cn").where(col("c02_level").gt(1500))
    ds.groupBy("cn").count().show()


  }

}
