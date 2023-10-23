package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
object UDFS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("udf")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    } //te da el cubo de un valor
    // Register UDF
    spark.udf.register("cubed", cubed)

    //Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    // Query the cubed UDF
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    //ahora lo intento hacer con la api de spark
   /* val cube=udf((s:Long)=>s*s*s, Long)
    val df= spark.range(1,9)
    val resultDF=df.select("id", cubed(col ("id")).alias("id_cubed"))
    resultDF.show()
    */
    spark.sql("SELECT id FROM udf_test WHERE id IS NOT NULL AND strlen(id)>1").show()

  }

}
