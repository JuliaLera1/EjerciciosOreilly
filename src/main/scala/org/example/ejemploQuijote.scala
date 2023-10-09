package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejemploQuijote {
  def Quijote(spark: SparkSession) {
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect = rdd.collect()
    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)

    val df = spark.createDataFrame(List(("A", 1), ("B", 2), ("C", 3)))
    df.withColumn("Test", lit("AAAA")).show()
    //df.write.csv("src/resources/dfPruebas")

    import spark.implicits._

    val df2 = Seq((1, "Uno"), (2, "Dos"), (3, "Tres")).toDF("Numero", "String")
    df2.show()

    val df3 = spark.read.text("src/main/resources/el_quijote.txt")
    df3.show()

  }
}
