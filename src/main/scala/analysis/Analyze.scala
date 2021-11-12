package analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import setup.SparkConnect

class Analyze   (){
    val sc = new SparkConnect()

    def q1(): Unit = {
        val spark = sc.getSparkSession()
        import spark.implicits._
        val inputDF = sc.getDataFrame()
        val modifiedDF = inputDF.withColumn("Year", year(to_date(col("Date"),"MM/dd/yyyy"))).withColumn("Month", month(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Day", dayofmonth(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Sales", col("Sale (Dollars)").cast(DoubleType)).withColumn("Volume", col("Volume Sold (Gallons)").cast(DoubleType))
        // dateSplitDF.select(col("Date"), col("Year"), col("Month"), col("Day")).show()
        val result1DF = modifiedDF.groupBy($"Year", $"Month").agg(sum($"Sales")/1000000 as "Total Sales (mil)", sum($"Volume")/53 as "Total Volume (Barrels)").sort(col("Year"), col("Month")).withColumn("Total Sales (mil)", format_number($"Total Sales (mil)", 2)).withColumn("Total Volume (Barrels)", regexp_replace(format_number($"Total Volume (Barrels)", 2), ",", ""))
        result1DF.show(110)
        result1DF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question1")
        // val dateColumnsDF = formattedDateDF.withColumn("year", year(col("value"))).withColumn("month", month(col("value"))).withColumn("day", dayofmonth(col("value"))).withColumn("WeekDay", dayofweek(col("value")))
        // dateColumnsDF.printSchema()
        // dateColumnsDF.show()
       
    }

    def q2(): Unit = {
        
    }

    def q3(): Unit = {
        
    }

    def q4(): Unit = {
        
    }

    def q5(): Unit = {
        
    }

    def q6(): Unit = {
        
    }

    def q7(): Unit = {
        
    }

    def q8(): Unit = {
        
    }

    def q9(): Unit = {
        
    }
}