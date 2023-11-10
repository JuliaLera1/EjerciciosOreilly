package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, functions, types}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._


object AppEjercicio {

  def main (args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
      .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sc.getConf)
    .master("local[1]")
      .appName("PivotajeEjercicio")
    //.enableHiveSupport()
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    //Creamos un Schema y se lo pasamos para que no salga una columna corrupt_record
    val schemaventas= StructType(Array(
      StructField("producto_id", StringType, true),
      StructField("fecha", DateType, true),
      StructField("cantidad_vendida", IntegerType, true),
      StructField("region_id", StringType, true)
    ))

    //Carga los datos desde el archivo JSON
    val ventas = spark.read.schema(schemaventas).json("src/main/resources/tienda/landing/ventas.json")


    //Registra la tabla para poder ejecutar consultas SQL
    ventas.createOrReplaceTempView("ventas1")

    //Quitamos los datos con el campo fecha o región nulo
    val ventasDF = spark.sql("SELECT * FROM ventas1 WHERE fecha IS NOT NULL AND region_id IS NOT NULL")

    ventasDF.createOrReplaceTempView("ventas")
//Escribimos el DataFrame en formato Parquet, particionado por fecha
    ventasDF.write
      .partitionBy("fecha")
      .mode("overwrite")
      .parquet("src/main/resources/tienda/staging/ventas")
//ahora cargamos el archivo json con los datos de los productos
    val productos= spark.read.json("src/main/resources/tienda/productos.json")
    productos.createOrReplaceTempView("productos")

    //hacemos un join para que en la tabla de las ventas nos aparezca a que categoría pertenece el artículo comprado, solo nos interesan los campos fecha y categoría así que no seleccionamos el resto
    val ventascategoria=ventasDF.as("v").join(productos.as("a"), col("a.producto_id")=== col("v.producto_id"))
      .select("a.categoria", "v.fecha", "v.cantidad_vendida", "v.region_id")
    ventascategoria.createOrReplaceTempView("VentasConCategoria")

    //ahora calculamos la cantidad de productos agrupados por categoría y fecha

    val resumen_ventas_1=spark.sql(
      """
        |SELECT categoria, fecha, SUM(cantidad_vendida) AS ventas_cnt
        |FROM VentasConCategoria
        |GROUP BY categoria, fecha
        |ORDER BY fecha
        |""".stripMargin
    )
    resumen_ventas_1.show()

    val regiones=spark.read.json("src/main/resources/tienda/regiones.json")
    regiones.createOrReplaceTempView("regiones")
    val ventasRegion=ventascategoria.as("v").join(regiones.as("r"), col("v.region_id")=== col("r.region_id"))
      .select("r.nombre_region", "v.categoria", "v.cantidad_vendida")
    ventasRegion.createOrReplaceTempView("ventasRegion")

    //este código nos da el porcentaje de productos vendidos en cada region, sin distincion de categoría
    println("En esta tabla, simplemente se muestra el porcentaje de producto total, sin distincion de categoria, que se ha vendido en cada region: \n")
spark.sql(
      """
        |SELECT nombre_region, (sum(cantidad_vendida)*100/(SELECT SUM(cantidad_vendida) FROM ventasRegion)) as ventas_prc
        |FROM ventasRegion
        |GROUP BY nombre_region
        |""".stripMargin
    )



  // primero, para aclararnos, hacemos una consulta donde calculamos cuanto se ha vendido de cada producto en cada region
    println("En esta tabla, se muestra cuánto se ha vendido de cada producto en cada region: \n")
    spark.sql(
      """
        |SELECT categoria, nombre_region, SUM(cantidad_vendida) as total_vendido
        |FROM ventasRegion
        |GROUP BY categoria, nombre_region
        |""".stripMargin
    ).show()

    //Ahora ya podemos calcular los porcentajes...
    println("En esta tabla se muestra la cantidad total de cada tipo de producto vendido en cada region y el porcentaje (el porcentaje " +
      "se calcula sobre la cantidad de producto de esa categoría total vendido en las cuatro regiones): \n")
    val resumen_ventas_2=
   spark.sql(
     """
       |SELECT categoria, nombre_region, total_vendido,
       |total_vendido*100 / SUM(total_vendido) OVER(PARTITION BY categoria) AS porcentaje_vendido
       |FROM (
       |    SELECT nombre_region,
       |           categoria,
       |           SUM(cantidad_vendida) AS total_vendido
       |    FROM ventasRegion
       |    GROUP BY categoria, nombre_region
       |)
       |ORDER BY categoria
       |""".stripMargin
   )
    resumen_ventas_2.show()



    resumen_ventas_1.write
      .option("header", "true") //esto hace que se incluya el encabezado en el csv
      .option("delimeter", ";") //establece los delimitadores como ;
      .mode("overwrite")
      .csv("src/main/resources/tienda/business/resumen_ventas_1")
    resumen_ventas_2.write
      .option("header", "true") //esto hace que se incluya el encabezado en el csv
      .option("delimeter", ";") //establece los delimitadores como ;
      .mode("overwrite")
      .csv("src/main/resources/tienda/business/resumen_ventas_2")




  }

}
