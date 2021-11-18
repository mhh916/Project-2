package analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import setup.SparkConnect
import org.apache.spark.sql.types.DecimalType

class analysisP213(spark:SparkConnect) {
  
  def q4(): Unit = {
    println("Loading dataFrame...")
    val df1 = spark.getDataFrame()
    //process RDD. Add columns, transform columns, and aggregate. Question 4 part 1&2
    val df2 = df1.withColumn("PPLiter (L/Per Dollar)", df1("Sale") / df1("VolumeSoldL")).withColumn("Category Name", lower(df1("CategoryName"))).groupBy("CategoryName").agg(avg("PPLiter (L/Per Dollar)").cast(DecimalType(10,2)).as("AVG_Price"))
    println("Most Expensive Liquor Category Name (Dollar/Liter)")
    //display top 5 results to Most Expensive Liquor
    df2.orderBy(col("AVG_Price").desc).show(5,false)
    println("Least Expensive Liquor Category Name (Dollar/Liter)")
    //dispay top 5 results to Least Expensive Liquor
    df2.orderBy(col("AVG_Price").asc).show(5,false)
    println("Most Popular Liquor by Volume(Barrels)")
    //process RDD. Add columns, transform columns, and aggregate. Question 4 part 3
    val df3 = df1.withColumn("Category Name", lower(df1("CategoryName"))).withColumn("Volume Sold (Barrels)",df1("VolumeSoldG")/53).groupBy("CategoryName").agg(sum("Volume Sold (Barrels)").cast(DecimalType(10,2)).as("Total Volume (Barrels)")).orderBy(col("Total Volume (Barrels)").desc)
    df3.show(5,false)
    }
}
