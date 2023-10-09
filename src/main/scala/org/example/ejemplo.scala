package org.example

import org.apache.spark.sql.functions.{avg, col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ejemplo {
  def ejecutable(spark: SparkSession){
  //create a DataFrame using SparkSession
  //create a DataFrame of names and ages
  val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Deny", 31), ("Jules", 30),
    ("TD", 35))).toDF("Name", "Age")
  //Group the same names together, aggregate their ages and compute an average
  val avgDF = dataDF.groupBy("Name").agg(avg("Age"))
  //Show the results of the final execution
  avgDF.show()
val schema = StructType(Array(StructField("author", StringType,false),
  StructField("title", StringType, false), StructField("pages",IntegerType,false)))

    val schemaDDL = "author STRING, title STRING, pages INT"

    funcion(dataDF).show()
  }

  def funcion(df: DataFrame): DataFrame = {
    df.withColumn("Age3", col("Age")*3)

  }
}

