package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

import java.util.Properties

object transformacionVentas {
  def ejTransformacion(propertiesFile: String)(implicit spark: SparkSession): Unit={

    val properties = new java.util.Properties()
    val input = new java.io.FileInputStream(propertiesFile)
    properties.load(input)

    //aquí ponemos todos los path refiriendonos al properties.file
    val ventasPath = properties.getProperty("ventasFile")
    val productosPath = properties.getProperty("productosFile")
    val regionesPath = properties.getProperty("regionesFile")
    val validacionParquet = properties.getProperty("validacionPath")
    val csv1path = properties.getProperty("csv1Path")
    val csv2path = properties.getProperty("csv2Path")

    //Creamos un Schema y se lo pasamos para que no salga una columna corrupt_record
    val schemaventas = StructType(Array(
      StructField("producto_id", StringType, true),
      StructField("fecha", DateType, true),
      StructField("cantidad_vendida", IntegerType, true),
      StructField("region_id", StringType, true)
    ))

    //Carga los datos desde los archivos JSON a un DataFrame
    val ventas = spark.read.schema(schemaventas).json(ventasPath)
    val productos = spark.read.json(productosPath)
    val regiones = spark.read.json(regionesPath)

    //Registra las tablas para poder ejecutar las consultas SQL
    ventas.createOrReplaceTempView("ventas1")
    productos.createOrReplaceTempView("productos")
    regiones.createOrReplaceTempView("regiones")

    //TRANSFORMACIÓN: Quitamos los datos con el campo fecha o región nulo
    val ventasDF = spark.sql("SELECT * FROM ventas1 WHERE fecha IS NOT NULL AND region_id IS NOT NULL")
    ventasDF.createOrReplaceTempView("ventas")
    //Escribimos el DataFrame en formato Parquet, particionado por fecha
    ventasDF.write
      .partitionBy("fecha")
      .mode("overwrite")
      .parquet(validacionParquet)

    //Hacemos un join para que en la tabla de las ventas nos aparezca a que categoría pertenece el artículo comprado, solo nos interesan los campos fecha y categoría así que no seleccionamos el resto
    val ventascategoria = ventasDF.as("v").join(productos.as("a"), col("a.producto_id") === col("v.producto_id"))
      .select("a.categoria", "v.fecha", "v.cantidad_vendida", "v.region_id")
    ventascategoria.createOrReplaceTempView("VentasConCategoria")

    //Ahora calculamos la cantidad de productos agrupados por categoría y fecha (RESUMEN_VENTAS_1)
    val resumen_ventas_1 = spark.sql(
      """
        |SELECT categoria, fecha, SUM(cantidad_vendida) AS ventas_cnt
        |FROM VentasConCategoria
        |GROUP BY categoria, fecha
        |ORDER BY fecha
        |""".stripMargin
    )
    println("En esta tabla se muestra la cantidad de productos venidos agrupados por categoría y fecha: \n")
    resumen_ventas_1.show()

    //Ahora hacemos otro join a la tabla que unía ventas y productos para que aparezca el nombre de la región donde se realizó la venta
    val ventasRegion = ventascategoria.as("v").join(regiones.as("r"), col("v.region_id") === col("r.region_id"))
      .select("r.nombre_region", "v.categoria", "v.cantidad_vendida")
    ventasRegion.createOrReplaceTempView("ventasRegion")

    println("En esta tabla, simplemente se muestra el porcentaje de producto total, sin distincion de categoria, que se ha vendido en cada region: \n")
    spark.sql(
      """
        |SELECT nombre_region, (sum(cantidad_vendida)*100/(SELECT SUM(cantidad_vendida) FROM ventasRegion)) as ventas_prc
        |FROM ventasRegion
        |GROUP BY nombre_region
        |""".stripMargin
    )

    // Primero, hacemos una consulta donde calculamos cuanto se ha vendido de cada producto en cada región
    println("En esta tabla, se muestra cuánto se ha vendido de cada producto en cada región: \n")
    spark.sql(
      """
        |SELECT categoria, nombre_region, SUM(cantidad_vendida) as total_vendido
        |FROM ventasRegion
        |GROUP BY categoria, nombre_region
        |""".stripMargin
    ).createOrReplaceTempView("VentasTotVendido")

    //Ahora ya podemos calcular los porcentajes de cada producto vendidos en cada región
    println("En esta tabla se muestra la cantidad total de cada tipo de producto vendido en cada región y el porcentaje (el porcentaje " +
      "se calcula sobre la cantidad de producto de esa categoría total vendido en las cuatro regiones): \n")
    val resumen_ventas_21 =
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
    resumen_ventas_21.show()
    resumen_ventas_21.createOrReplaceTempView("resumen_ventas_21")
    //Ahora tengo que hacer un pivot
    val resumen_ventas_2 = {
      spark.sql(
        """
          |SELECT *
          |FROM ( SELECT categoria, nombre_region,
          |total_vendido*100 / SUM(total_vendido) OVER(PARTITION BY categoria) AS porcentaje_vendido
          |FROM VentasTotVendido)
          |PIVOT (
          | CAST(MAX(porcentaje_vendido) AS DECIMAL(5,2) ) AS ventas_prc
          | FOR nombre_region IN ("Norte", "Sur", "Este", "Oeste")
          |)
          |ORDER BY categoria
          |
          |""".stripMargin
      )
    }
    println("Aquí se muestra el tipo de producto y el porcentaje del mismo vendido en cada región: ")
    resumen_ventas_2.show()

    //Por último, queremos guardar las tablas en formato csv en nuestra interfaz.
    resumen_ventas_1.write
      .option("header", "true") //esto hace que se incluya el encabezado en el csv
      .option("delimeter", ";") //establece los delimitadores como ;
      .mode("overwrite")
      .csv(csv1path)
    println(s"La tabla con la cantidad de producto vendidos de cada tipo en cada fecha ha sido almacenada en formato csv en el path: $csv1path")
    resumen_ventas_2.write
      .option("header", "true") //esto hace que se incluya el encabezado en el csv
      .option("delimeter", ";") //establece los delimitadores como ;
      .mode("overwrite")
      .csv(csv2path)
    println(s"La tabla donde se muestra la categoría del producto y el porcentaje del mismo vendido en cada región ha sido almacenada en formato csv en el path: $csv2path")

  }

}
