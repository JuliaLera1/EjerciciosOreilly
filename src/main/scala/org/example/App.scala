package org.example

import org.apache.hadoop.shaded.com.google.common.io.MoreFiles
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession




object App {

  def main(args: Array[String]) {

    //empezamos la sesión de Spark
//    val spark: SparkSession = SparkSession.builder().master("local[1]")
//      .appName("SparkByExamples")
//      .getOrCreate()
val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
  .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)

    implicit val spark: SparkSession = SparkSession.builder().config(sc.getConf).master("local[1]")
      .appName("SparkByExamples.com")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    //aquí ponemos cada función a la que vamos a llamar

    //ejemplo.ejecutable(spark)
    //ejemploQuijote.Quijote(spark)
    //ejemplo2.ej2(spark)
    //ejemplo3.ej3(spark)
    //ejSanFran.Fire(spark)
    //MnM.basico(spark)
    //IotDevices.arcjson(spark)
    //MoreFunctions.execute()
    //ExArray.ex1()
    //ExArray.avanzado()
    airports.airpots()


  }


}

