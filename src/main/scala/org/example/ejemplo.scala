package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ejemplo {
  def main(args: Array[String]){
  //create a DataFrame using SparkSession
val spark = SparkSession
  .builder().master("local[1]") //me faltaba el master
  .appName("AuthorsAges")
  .getOrCreate()
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
  }
}

