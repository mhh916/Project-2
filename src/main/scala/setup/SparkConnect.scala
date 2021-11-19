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
      //df = spark.read.format("csv").option("header", "true").load("file:///home/maria_dev/Project2/Iowa_Liquor_Sales.csv")
      try {
        df = spark.read.parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Project2/Parquet/liqour.parquet")
      } catch {
        case e: Throwable => {
          df = spark.read.format("csv").option("header", "true").load("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Project2/Iowa_Liquor_Sales.csv")
          println("Converting CSV to Parquet...")
          makeParquet()
          println("Loading parquet into DataFrame...")
          df = spark.read.parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Project2/Parquet/liqour.parquet")
        }
      }

    } catch {
      case e: Throwable => println(e)
    }
    

    def getSparkSession(): SparkSession = {
        spark
    }

    def getDataFrame(): DataFrame ={
      df
    }

    def makeParquet(): Unit = {
      df = df.withColumnRenamed("Invoice/Item Number", "InvoiceNumber")
      df = df.withColumnRenamed("Store Number", "StoreNumber")
      df = df.withColumnRenamed("Store Name", "StoreName")
      df = df.withColumnRenamed("Zip Code", "ZipCode")
      df = df.withColumnRenamed("Store Location", "StoreLocation")
      df = df.withColumnRenamed("County Number", "CountyNumber")
      df = df.withColumnRenamed("Category Name", "CategoryName")
      df = df.withColumnRenamed("Vendor Number", "VendorNumber")
      df = df.withColumnRenamed("Vendor Name", "VendorName")
      df = df.withColumnRenamed("Item Number", "ItemNumber")
      df = df.withColumnRenamed("Item Description", "ItemDescription")
      df = df.withColumnRenamed("Bottle Volume (ml)", "BottleVolume")
      df = df.withColumnRenamed("State Bottle Cost", "StateBottleCost")
      df = df.withColumnRenamed("State Bottle Retail", "StateBottleRetail")
      df = df.withColumnRenamed("Bottles Sold", "BottlesSold")
      df = df.withColumnRenamed("Sale (Dollars)", "Sale")
      df = df.withColumnRenamed("Volume Sold (Liters)", "VolumeSoldL")
      df = df.withColumnRenamed("Volume Sold (Gallons)", "VolumeSoldG")
      df.write.parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Project2/Parquet/liqour.parquet")
    }



   





}