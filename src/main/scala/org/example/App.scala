package org.example

import org.apache.hadoop.shaded.com.google.common.io.MoreFiles
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveSessionStateBuilder
//import org.apache.hadoop.hive.conf.HiveConf




object App {

  def main(args: Array[String]) {

    //Ponemos los args en la configuración de las variables, para que scala las coja y haga lo que toca.
    //Hay dos opciones. La primera es hacer un bucle if al definir las variables.
    //val (fase, propertiesFile) = if (args.length >= 2) (args(0), args(1)) else ("", "")

    //La otra forma, algo más clara:
    //Primero inicializamos nuestras variables (var porque las vamos a cambiar no son constantes)
    var fase=""
    var propertiesFile= ""
    //Después, creamos un bucle if que verifica si se proporcionan los argumentos necesarios y actualiza los valores
    if (args.length >= 2) {
      println("Tomamos los argumentos de la configuración.")
      fase=args(0) //no necesito comillas porque scala interpreta los args como string
      propertiesFile=args(1)
    } //puedo hacer que el tercer argumento no sea necesario para realizar el proceso, solo se ponga para que en vez de hacerlo con spark.sql lo haga con la spark api
    else {
      println("No se proporcionaron suficientes argumentos. Por favor, asegúrese de haber indicado al menos 2 argumentos en la configuración.")
    }

    //Ahora, vamos a imprimir los valores para que el usuario se asegure de que se está haciendo lo que quiere.
    println(s"La fase que se va a realizar es: $fase, y la ruta desde donde tomamos el properties.file es: $propertiesFile.")


    val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
        .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)

    implicit val spark: SparkSession = SparkSession.builder().config(sc.getConf).master("local[1]")
      .appName("SparkByExamples.com")
      //.enableHiveSupport()
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse") // Set Hive warehouse directory
//      .config("hive.metastore.uris", "thrift://your-metastore-uri:9083") // Set Hive Metastore URI
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    /*
    Ejercicio que nos mandó David sobre pivotaje. El primer script hace los tres primeros ejercicios pasándole todos los paths en el script.
    En el segundo, los paths están en el properties.file y solo le pasamos el path de ese archivo.
    La última parte es el ejercicio al completo:
          Adaptar el proceso para que, mediante los argumentos recibidos en la clase main, el proceso
          ejecute la validación o la transformación (args(0)) y reciba el path de un archivo llamado
          “properties.file” (args(1)), mediante el cual obtenga los paths de entrada y salida, la base de
          datos de Hive y sus tablas, para que el proceso funcione correctamente.
    */

    //Ejercicio123.ejecutable()
    //EjercicioScript.ejecutable()

    fase match{
      case "validacion" => validacionVentas.ejValidacion(propertiesFile)
      case "transformacion" => transformacionVentas.ejTransformacion(propertiesFile)
      case _ => println("La fase especificada no es ninguna de las reconocidas.")
    }


// ESTOS SON OTROS SCRIPT PARA PRACTICAR.
    //aquí ponemos cada función a la que vamos a llamar
    //SWITCH CASE PARA que llame a cada script
    //ejemplo.ejecutable(spark)
    //ejemploQuijote.Quijote(spark)
    //ejemplo2.ej2(spark)
    //ejemplo3.ej3(spark)
    //ejSanFran.Fire(spark)
    //MnM.basico(spark)
    //IotDevices.arcjson(spark)
    //MoreFunctions.execute() //no funciona!!
    //ExArray.ex1()
    //ExArray.avanzado()
    //airports.airpots()
    //bloggers.ejblog()
    //Datasets.randomDS()


  }


}

