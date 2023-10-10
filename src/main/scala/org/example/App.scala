package org.example

import org.apache.spark.sql.SparkSession



object App {

  def main(args: Array[String]) {

    //empezamos la sesión de Spark
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //aquí ponemos cada función a la que vamos a llamar

    //ejemplo.ejecutable(spark)
    //ejemploQuijote.Quijote(spark)
    //ejemplo2.ej2(spark)
    //ejemplo3.ej3(spark)
    ejSanFran.Fire(spark)


  }

}

