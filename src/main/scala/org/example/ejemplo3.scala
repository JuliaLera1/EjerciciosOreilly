package org.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types._



object ejemplo3 {
  def ej3(spark: SparkSession): Unit = {

    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))
//    println(blogRow) //problemas para mostrar el array
//    hacemos lo siguiente
    for (i<- 0 to 6) {
      blogRow(i) match {
       case arr: scala.Array[String] => arr.foreach(e => print(e + ", "))
       case _ => print(blogRow(i) + ", ")
     }
    }
//    def printbon(iterable: Iterable[Any])={
//      iterable.foreach(print)
//    }
//    printbon(blogRow(6))
//
//    for (i <- 0 to 6) {
//      blogRow(i) match {
//        case arr: scala.Iterable[_] => printbon(arr)
//        case _ => print(blogRow(i) + ", ")
//      }
//    }


    // Si en vez de array ponemos List, sí implementa directamente a string, no tenmos que hacer nada más que print


    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = spark.createDataFrame(rows).toDF("Author", "State")
    authorsDF.show()


  }

}
