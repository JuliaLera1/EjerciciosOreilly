package org.example

import org.apache.hadoop.shaded.com.google.common.io.MoreFiles
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveSessionStateBuilder
//import org.apache.hadoop.hive.conf.HiveConf




object App {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
        .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)

    implicit val spark: SparkSession = SparkSession.builder().config(sc.getConf).master("local[1]")
      .appName("SparkByExamples.com")
      //.enableHiveSupport()
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse") // Set Hive warehouse directory
//      .config("hive.metastore.uris", "thrift://your-metastore-uri:9083") // Set Hive Metastore URI
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    //aquí ponemos cada función a la que vamos a llamar
//SWITCH CASE PARA que llame a cada script
    //ejemplo.ejecutable(spark)
    //ejemploQuijote.Quijote(spark)
    //ejemplo2.ej2(spark)
    //ejemplo3.ej3(spark)
    //ejSanFran.Fire(spark)
    //MnM.basico(spark)
    //IotDevices.arcjson(spark)
    //MoreFunctions.execute() //no funciona!!
    //ExArray.ex1()
    //ExArray.avanzado()
    //airports.airpots()
    //bloggers.ejblog()
    //Datasets.randomDS()

    /*
    Ejercicio que nos mandó David sobre pivotaje y utilizar spark de forma más segura y eficiente

    */

    //Ejercicio123.ejecutable()
    EjercicioScript.ejecutable()


  }


}

