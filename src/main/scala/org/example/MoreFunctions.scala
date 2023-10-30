package org.example

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.DataFrame

object MoreFunctions {
  def execute()(implicit spark: SparkSession): Unit = {
    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val rows=Seq(t1,t2)
//    val tC = spark.createDataFrame(rows).toDF("celsius")
//    tC.createOrReplaceTempView("tC")
//    // Show the DataFrame
//    tC.show()
    spark.sql("CREATE DATABASE IF NOT EXISTS ejercicios")
    spark.sql("USE ejercicios")

    spark.sql("CREATE TABLE IF NOT EXISTS personas (nombre String, edad Int)")


  }

}
