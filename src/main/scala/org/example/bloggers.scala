package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object bloggers {
  def ejblog()(implicit spark: SparkSession): Unit={
    import spark.implicits._

    case class Bloggers(id: IntegerType, first: StringType, last: StringType, url: StringType, date: StringType,
                        hits: IntegerType, campaigns: Array[StringType])
    val bloggers = "src/main/resources/blogs.json"
//    val bloggersDS = spark.read
//      .format("json")
//      .option("path", bloggers)
//      .load()
//      .as[Bloggers]


  }

}
