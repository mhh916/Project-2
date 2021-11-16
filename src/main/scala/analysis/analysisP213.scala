package analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import setup.SparkConnect


class analysisP213(spark:SparkConnect) {
  
  def q4(): Unit = {
    //move file to /user/maria_dev/Project2/Iowa_Liquor_Sales.csv
    //val df1 = spark.read.option("header",true).option("inferSchema",true).csv("/user/maria_dev/Project-2zb/Iowa_Liquor_Sales.csv")
    println("Loading dataFrame...")
    val df1 = spark.getDataFrame()
    val df2 = df1.withColumn("PPLiter (L/Per Dollar)", df1("Sale (Dollars)") / df1("Volume Sold (Liters)"))
    val df3 = df2.groupBy("Category Name").agg(avg("PPLiter (L/Per Dollar)").as("AVG_Price"))
    println("Most Expensive Liquor (Category Name)")
    df3.orderBy(col("AVG_Price").desc).show(5)
    println("Least Expensive Liquor Category")
    df3.orderBy(col("AVG_Price").asc).show(5)
    val df4 = df1.groupBy("Category Name").agg(sum("Volume Sold (Liters)").as("SUM_Volume(L)"))
    println("Most Popular Liquor by Volume(Liters)")
    df4.orderBy(col("SUM_Volume(L)").desc).show(5)
    }
  
}
