package org.example

import org.apache.spark.sql.SparkSession
import scala.util.Random._


object Datasets {
  case class Usage(uid: Int, uname: String, usage: Int)
  def randomDS()(implicit spark: SparkSession): Unit={
    val enc = org.apache.spark.sql.Encoders.product[Usage]

    // Our case class for the Dataset

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)


  }

}
