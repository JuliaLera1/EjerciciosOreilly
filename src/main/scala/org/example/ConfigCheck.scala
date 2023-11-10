package org.example

object ConfigCheck {
  import org.apache.spark.sql.SparkSession
  //Aquí estamos creando una función en nuestra aplicación para que pasándole la sesión de Spark nos de su configuración
  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }
  //Ahora creamos otra funcion, iniciamos una sesion con una configuración particular y pedimos que nos devuelva cuál es esa config.
  def main(args: Array[String]) {
    // Create a session
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", 5) //indicamos cuantas particiones queremos que spark cree
      .config("spark.executor.memory", "2g") //cambia la memoria de spark executor
      .master("local[*]")
      .appName("SparkConfig")
      .getOrCreate()
    printConfigs(spark) //llamamos a la funcion anterior para que nos de la config de la sesion
    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)  //volvemos a cambiar la config de particiones a default
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark) //volvemos a llamar a la función, observa como ha cambiado lo de las particiones

    //esto es para ver unicamente los Spark-SQL configs específicos
    spark.sql("SET -v").select("key", "value").show(5, false)

  }
}
