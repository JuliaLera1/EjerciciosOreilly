package org.example

import org.apache.spark.sql.SparkSession
import java.util.Properties

object EjercicioScript {
  def ejecutable()(implicit spark: SparkSession): Unit={

    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("src/main/resources/properties.file"))

  }

}
