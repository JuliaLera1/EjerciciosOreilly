package org.example

import org.apache.spark.sql.SparkSession

object leer {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Leer")
      .getOrCreate()

    val rutaArchivoJSON = "C:/users/julia.lera/Desktop/miarchivo.json"
    val archivo = spark.read
      .option("header", "true")
      .json(rutaArchivoJSON)
    archivo.show()
  }

}
