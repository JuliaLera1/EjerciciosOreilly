package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.source.image



object DataSources {

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Cap4read")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //reading data from a data source into a data frame
    // Use CSV
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("src/main/resources/csv/2010-summary.csv")


/*
No puedo guardar el df ahora mismo porque los archivos se guardan en HDFS, no en Windows normal
    val location = ""
    df.write.format("json").mode("overwrite").save(location)
*/

    // Use Parquet
    val file = "src/main/resources/summary.parquet"
    //Parquet is the default data source in Spark, you can only see its content with Spark (it is thought for the machine to understand)
    val df1 = spark.read.format("parquet").load(file)


    //use JSON
    val filejson = "src/main/resources/json/2010-summary.json" //En el libro, ponen /* pero aquí no funciona, por qué?
    val df2 = spark.read.format("json").load(filejson)

    //cargar una imagen
    val imageDir = "src/main/resources/Captura1.PNG"
    val image1 = "src/main/resources/Browse2frame0000.jpg"
    val imagesDF = spark.read.format("image").load("image1")
    imagesDF.printSchema()



  }
}
