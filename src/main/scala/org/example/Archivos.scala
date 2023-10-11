package org.example

import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Json
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object Archivos {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Archivos")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Crear un objeto JSON utilizando org.json4s.jackson.JsonMethods
    val jsonData = ("nombre" -> "Ejemplo") ~ ("edad" -> 30) ~ ("ciudad" -> "Ciudad de Ejemplo")

    // Convierte el objeto JSON en una cadena JSON
    val jsonString = compact(render(jsonData))

    // Especifica la ruta y el nombre del archivo JSON que deseas crear
    val rutaArchivoJSON = "C:/users/julia.lera/Desktop/miarchivo.json"

    // Escribe el JSON en el archivo
    import java.io.PrintWriter
    val archivoJSON = new PrintWriter(rutaArchivoJSON)
    archivoJSON.write(jsonString)
    archivoJSON.close()


  }

}
