package setup

import org.apache.spark.sql.SparkSession
// For implicit conversions from RDDs to DataFrames
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

class SparkConnect(){
    var spark: SparkSession = null
    var df: DataFrame = null
    try {
      spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    
      spark.sparkContext.setLogLevel("ERROR")
      df = spark.read.format("csv").option("header", "true").load("file:///home/maria_dev/Project2/Iowa_Liquor_Sales.csv")
    } catch {
      case e: Throwable => println(e)
    }
    

    def getSparkSession(): SparkSession = {
        spark
    }

    def getDataFrame(): DataFrame ={
      df
    }



   





}