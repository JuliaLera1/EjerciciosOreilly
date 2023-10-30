package org.example

import org.apache.spark.sql.SparkSession

object ExArray {
  def ex1()(implicit spark: SparkSession): Unit = {

    val hobbies= Seq(
      ("María",20, Seq("Baloncesto", "Baile"), Seq("Mates", "Lengua")),
      ("Paula",23, Seq("Tenis", "Piano", "Pintura"), Seq("Francés", "inglés")),
      ("Alex", 25, Seq("Baloncesto", "Guitarra"), Seq("Mates", "Biología")),
        ("Fran", 22, Seq("Tenis", "Baile"), Seq("Lengua", "Historia")))
    val colnames= Seq("Nombre", "Edad", "Hobbies", "Asignaturas")
    val hobbiesDF=spark.createDataFrame(hobbies).toDF(colnames: _*)
    hobbiesDF.createOrReplaceTempView("hobbies")
     spark.sql("select array_intersect(Hobbies, Asignaturas) AS Mezcla FROM hobbies").show()
    spark.sql("SELECT array_distinct(Hobbies) AS AllHobbies FROM hobbies").show()
    spark.sql(
      """
        SELECT Hobbies, filter(Hobbies, h-> h="Baloncesto") as Basket
        from hobbies
        """
    ).show()
    spark.sql(
      """
        SELECT Hobbies, filter(Hobbies, h-> h in("Baloncesto", "Piano")) as BasketPiano
        from hobbies
        """
    ).show()


  }
def avanzado()(implicit spark: SparkSession): Unit={
  val temp = Seq(
    (1, Seq(35, 36, 32, 30, 40, 42, 38)),
    (2, Seq(31, 32, 34, 55, 56))
  )
//  val t1 = Array(35, 36, 32, 30, 40, 42, 38)
//  val t2 = Array(31, 32, 34, 55, 56)
//  val tC = Seq(t1, t2)
//  val df=

  val colnom = Seq("id", "celsius")
  val tempDF = spark.createDataFrame(temp).toDF(colnom: _*)
  tempDF.createOrReplaceTempView("temperaturas")
//hacemos un transform para ver las temperaturas en fahrenheit
  spark.sql(
  """
     SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
      FROM temperaturas
    """).show()
  // Filter temperatures > 38C for array of temperatures
  spark.sql(
    """
  SELECT celsius,
   filter(celsius, t -> t > 38) as high
   FROM temperaturas
  """).show()
  // exists devuelve true si existe un valor que cumpla la función especificada en el array
  spark.sql("""
SELECT celsius,
 exists(celsius, t -> t = 35) as threshold
 FROM temperaturas
""").show()

  //reduce() reduce los elementos del array a un unico valor uniendo los elementos a un buffer B
  //usando la funcion<B, T, B> y después aplica una funcion
  //function<B, R> en el buffer final:
  //aquí por ejemplo está calculando la media de temperaturas y luego pasándolas a fahrenheit
  spark.sql("""
SELECT celsius,
 reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit
 FROM temperatura
""").show()

}


}
