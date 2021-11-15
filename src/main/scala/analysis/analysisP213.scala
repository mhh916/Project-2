package analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import setup.SparkConnect


class analysisP213(spark:SparkConnect) {
  
  def q4(): Unit = {
    //move file to /user/maria_dev/Project2/Iowa_Liquor_Sales.csv
    //val df1 = spark.read.option("header",true).option("inferSchema",true).csv("/user/maria_dev/Project-2zb/Iowa_Liquor_Sales.csv")
    val df1 = spark.getDataFrame()
    val df2 = df1.withColumn("PPLiter (L/Per Dollar)", df1("Sale (Dollars)") / df1("Volume Sold (Liter)"))
    val df3 = df2.groupBy("Category").agg(avg("PPLiter (P/Per Dollaar)").as("AVG_Price"))
    df3.orderBy(col("AVG_PPLiter").desc).show(5)
    df3.orderBy(col("AVG_PPLiter").asc).show(5)
    val df4 = df1.groupBy("Category").agg(sum("Volume Sold (Liters)")).as("SUM_Volume(L)")
    df4.orderBy(col("SUM_Value(L)").desc).show(5)
    }
  
}
