package org.example

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.concat

object ejemplo2 {
  def ej2(spark: SparkSession): Unit = {

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


    // Get the column names as an array of strings
    val columnNames: Array[String] = blogsDF.columns

    // Print the column names
    columnNames.foreach(println)

    // Use col to compute value
    blogsDF.select(col("Hits") * 2).show(2)

    // Calcular la columna "Big Hitters" (mayores de 10000 Hits)
    val resultDF = blogsDF.withColumn("Big Hitters", expr("Hits > 10000"))

    // Mostrar el resultado
    resultDF.show()

    // Concatenate three columns, create a new column, and show only the newly created concatenated column
    val result1DF = blogsDF.withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
      .select(col("AuthorsId"))
      .show(4)

    // Sort by column "Id" in descending order
    blogsDF.sort(col("Id").desc).show()



  }
}
