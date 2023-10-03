package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ejemplo2 {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("Authors")
      .getOrCreate()

    //Definir un schema
    val schema = StructType(Seq(
      StructField("ID", IntegerType, nullable=false),
      StructField("First", StringType, nullable=false),
      StructField("Last", StringType, nullable = false),
      StructField("URL", StringType, nullable=false),
      StructField("Published", StringType, nullable=false),
      StructField("Hits", IntegerType, nullable=false),
      StructField("Campaigns", ArrayType(StringType), nullable=false)
    ))

    //Crear una secuencia con datos
    val data = Seq(
    Row(1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, Array("twitter", "LinkedIn")),
    Row(2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, Array("twitter", "LinkedIn")),
    Row(3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, Array("web", "twitter", "FB", "LinkedIn")),
    Row(4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, Array("twitter", "FB")),
    Row(5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, Array("web", "twitter", "FB", "LinkedIn")),
    Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, Array("twitter", "LinkedIn"))
    )

    // Create a DataFrame using the schema defined above
    val blogsDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Show the DataFrame; it should reflect our table above
    blogsDF.show()
    // Print the schema used by Spark to process the DataFrame
    println(blogsDF.printSchema) //schema tomado por Spark
    println(blogsDF.schema) //schema que hemos creado


  }
}
