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
  def avanzado()(implicit spark: SparkSession): Unit = {

    val temp= Seq(
      (35, 36, 32, 30, 40, 42, 38),
      (31, 32, 34, 55, 56)
    )
    val colnombres=Seq("celsius")
    val tempDF=spark.createDataFrame(temp).toDF(colnombres: _*)
    tempDF.createOrReplaceTempView("temperaturas")

    tempDF.show()

  }

}
