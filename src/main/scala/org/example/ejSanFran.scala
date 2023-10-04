package org.example
import org.apache.spark.sql.SparkSession
object ejSanFran {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("Fire")
      .getOrCreate()

    val sampleDF = spark
      .read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("C:/users/julia.lera/downloads/Fire_incidents.csv")
    sampleDF.show()


  }

}
