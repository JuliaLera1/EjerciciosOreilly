package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders}

object bloggers {
  case class Bloggers(id: BigInt, first: String, last: String, url: String,
                      hits: BigInt, campaigns: Array[String])
  //definir el case class fuera de la función, necesita tenerlo a nivel global
  def ejblog()(implicit spark: SparkSession): Unit={

    val enc = org.apache.spark.sql.Encoders.product[Bloggers]

    val bloggers = "src/main/resources/blogs.json"
    val bloggersDS = spark.read
      .format("json")
      .option("path", bloggers)
      .load()
      .as(enc)
    bloggersDS.printSchema()


  }

}
