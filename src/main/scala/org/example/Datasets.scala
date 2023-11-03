package org.example

import org.apache.spark.sql.SparkSession
import scala.util.Random._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Datasets {
  // Our case class for the Dataset
  case class Usage(uid: Int, uname: String, usage: Int)

  def randomDS()(implicit spark: SparkSession): Unit={
 //según ChatGPT
    //necesitamos importar esto para poder usar map con un double más adelante
    import spark.implicits._
    //le damos el encoder implicito
    implicit val usageEncoder = Encoders.product[Usage]

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))

    // Create a Dataset of Usage typed data
    val dsUsage: Dataset[Usage] = spark.createDataset(data)
   //dsUsage.show(10)

    //FILTER
    println("Aquí se muestran los usuarios con tiempo de uso mayor a 900 minutos: \n")
    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    //Another way is to define a function and supply that function as an argument to filter():
    def filterWithUsage(u: Usage) = u.usage > 900
    dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)
    /*
    In the first case we used a lambda expression, {d.usage > 900}, as an argument to
    the filter() method, whereas in the second case we defined a Scala function, def
    filterWithUsage(u: Usage) = u.usage > 900. In both cases, the filter() method
    iterates over each row of the Usage object in the distributed Dataset and applies the
    expression or executes the function, returning a new Dataset of type Usage for rows
    where the value of the expression or function is true.
     */

    /*
    Lamdas and functional arguments can returned computed values too, not only boolean values
    En el siguiente ejemplo, usamos la funcion de orden superior map(), con el objetivo de encontrar el coste de uso
    para cada usuario cuyo uso es superior a x min, para poder ofrecerles un precio especial por min.
     */

    // Use an if-then-else lambda expression and compute a value
//    dsUsage.map(u => {
//        if (u.usage > 750) u.usage * .15 else u.usage * .50
//      })
//      .show(5, false)
    // Define a function to compute the usage
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    // Use the function as an argument to map()
    dsUsage.map(u => {
      computeCostUsage(u.usage)
    }).show(5, false)


  }

}
