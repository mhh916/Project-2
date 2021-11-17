package analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import setup.SparkConnect
import org.apache.spark.sql.types.DecimalType

class analysisP213(spark:SparkConnect) {
  
  def q4(): Unit = {
    //move file to /user/maria_dev/Project2/Iowa_Liquor_Sales.csv
    //val df1 = spark.read.option("header",true).option("inferSchema",true).csv("/user/maria_dev/Project-2zb/Iowa_Liquor_Sales.csv")
    println("Loading dataFrame...")
    val df1 = spark.getDataFrame()
    val df2 = df1.withColumn("PPLiter (L/Per Dollar)", df1("Sale (Dollars)") / df1("Volume Sold (Liters)"))
    val df3 = df2.groupBy("Category Name").agg(avg("PPLiter (L/Per Dollar)").as("AVG_Price"))
    println("Most Expensive Liquor (Category Name)")
    val df4 = df3.orderBy(col("AVG_Price").desc)
    val df5 = df3.orderBy(col("AVG_Price").asc)
    df4.withColumn("AVG_Price", df4("AVG_Price").cast(DecimalType(6,2))).show(5,false)
    println("Least Expensive Liquor Category")
    df5.withColumn("AVG_Price", df4("AVG_Price").cast(DecimalType(6,2))).show(5,false)
    val df6 = df1.groupBy("Category Name").agg(sum("Volume Sold (Liters)").as("SUM_Volume(L)"))
    println("Most Popular Liquor by Volume(Liters)")
    val df7 = df6.orderBy(col("SUM_Volume(L)").desc)
    df7.withColumn("SUM_Volume(L)", df7("SUM_Volume(L)").cast(DecimalType(25,2))).show(5,false)
    }
  
}
