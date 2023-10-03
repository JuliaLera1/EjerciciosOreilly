package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession


object ejemplo {
  def main(args: Array[String]) {
  //create a DataFrame using SparkSession
val spark = SparkSession
  .builder
  .appName("AuthorsAges")
  .getOrCreate()
  //create a DataFrame of names and ages
  val dataDF = spark.createDataFrame(Seq(("Booke", 20), ("Brooke", 25), ("Deny", 31), ("Jules", 30),
    ("TD", 35))).toDF("Name", "Age")
  //Group the same names together, aggregate their ages and compute an average
  val avgDF = dataDF.groupBy("Name").agg(avg("Age"))
  //Show the results of the final execution
  avgDF.show()

  }
}

