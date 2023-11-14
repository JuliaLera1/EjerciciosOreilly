package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.example.EjercicioScript.getClass

import java.util.Properties

object validacionVentas {

  def ejValidacion(propertiesFile: String)(implicit spark: SparkSession): Unit={

    val properties = new java.util.Properties()
    val input = new java.io.FileInputStream(propertiesFile)
    properties.load(input)

    //aquí ponemos todos los path refiriendonos al properties.file
    val ventasPath = properties.getProperty("ventasFile")
    val productosPath = properties.getProperty("productosFile")
    val regionesPath = properties.getProperty("regionesFile")
    val validacionParquet = properties.getProperty("validacionPath")

    //Creamos un Schema y se lo pasamos para que no salga una columna corrupt_record
    val schemaventas = StructType(Array(
      StructField("producto_id", StringType, true),
      StructField("fecha", DateType, true),
      StructField("cantidad_vendida", IntegerType, true),
      StructField("region_id", StringType, true)
    ))

    //Carga los datos desde los archivos JSON a un DataFrame. Solo cargamos el archivo Ventas porque para esta fase no necesitamos el resto.
    val ventasDF = spark.read.schema(schemaventas).json(ventasPath)

    //Registra la tabla Ventas para poder ejecutar la validación. No necesitamos el resto de tablas.
    ventasDF.createOrReplaceTempView("ventas1")

    //TRANSFORMACIÓN: Quitamos los datos con el campo fecha o región nulo
    val ventasValidadoDF = spark.sql("SELECT * FROM ventas1 WHERE fecha IS NOT NULL AND region_id IS NOT NULL")
    ventasValidadoDF.createOrReplaceTempView("ventas")
    //Escribimos el DataFrame en formato Parquet, particionado por fecha
    ventasValidadoDF.write
      .partitionBy("fecha")
      .mode("overwrite")
      .parquet(validacionParquet)
    println(s"El DataFrame validado (no contiene filas con el campo fecha o el campo region_id sin información) ha sido correctamente almacenado en: $validacionParquet")


  }

}
