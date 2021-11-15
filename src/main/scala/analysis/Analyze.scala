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
        //Split Date into separate columns for year, month, and day. Also cast Sales and Volume to Double.
        val modifiedDF = inputDF.withColumn("Year", year(to_date(col("Date"),"MM/dd/yyyy"))).withColumn("Month", month(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Day", dayofmonth(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Sales", col("Sale (Dollars)").cast(DoubleType)).withColumn("Volume", col("Volume Sold (Gallons)").cast(DoubleType))
        //Result for part 1, group by year+month, sum sales and volume. Sales in millions of dollars, volume in barrels (53gallons/barrel)
        val result1DF = modifiedDF.groupBy($"Year", $"Month").agg(sum($"Sales")/1000000 as "Total Sales (mil)", sum($"Volume")/53 as "Total Volume (Barrels)").sort(col("Year"), col("Month")).withColumn("Total Sales (mil)", format_number($"Total Sales (mil)", 2)).withColumn("Total Volume (Barrels)", regexp_replace(format_number($"Total Volume (Barrels)", 2), ",", ""))
        //Part 2
        val dayGroupDF = modifiedDF.groupBy($"Year", $"Month", $"Day").agg(sum($"Sales")/1000000 as "Total Sales (mil)", sum($"Volume")/53 as "Total Volume (Barrels)")
        //dayGroupDF.show()
        //Create DFs for each holiday (3 day block leading up to holiday, 12/25 and 1/1 excluded as they have no records)
        val stpatricksDF = dayGroupDF.filter("Month == 3 AND Day == 15 OR Month == 3 AND Day == 16 OR Month == 3 AND Day == 17").withColumn("Holiday", lit("St. Patrick's Day"))
        val cincodemayoDF = dayGroupDF.filter("Month == 5 AND Day == 3 OR Month == 5 AND Day == 4 OR Month == 5 AND Day == 5").withColumn("Holiday", lit("Cinco de Mayo"))
        val july4thDF = dayGroupDF.filter("Month == 7 AND Day == 2 OR Month == 7 AND Day == 3 OR Month == 7 AND Day == 4").withColumn("Holiday", lit("4th of July"))
        val halloweenDF = dayGroupDF.filter("Month == 10 AND Day == 29 OR Month == 10 AND Day == 30 OR Month == 10 AND Day == 31").withColumn("Holiday", lit("Halloween"))
        val xmasDF = dayGroupDF.filter("Month == 12 AND Day == 22 OR Month == 12 AND Day == 23 OR Month == 12 AND Day == 24").withColumn("Holiday", lit("Christmas"))
        val newyearsDF = dayGroupDF.filter("Month == 12 AND Day == 29 OR Month == 12 AND Day == 30 OR Month == 12 AND Day == 31").withColumn("Holiday", lit("New Year's"))
        
        // Shows for individual holidays
        // stpatricksDF.sort("Year", "Day").show(50)
        // cincodemayoDF.sort("Year", "Day").show(50)
        // july4thDF.sort("Year", "Day").show(50)
        // halloweenDF.sort("Year", "Day").show(50)
        // xmasDF.sort("Year", "Day").show(50)
        // newyearsDF.sort("Year", "Day").show(50)

        //Union all holiday DFs together and group by holiday and year, then sum sales and volume sold.
        val holidayDaysDF = stpatricksDF.union(cincodemayoDF).union(july4thDF).union(xmasDF).union(newyearsDF).union(halloweenDF)
        val result2DF = holidayDaysDF.groupBy($"Holiday", $"Year").agg(sum($"Total Sales (mil)") as "Total Sales (mil)", sum($"Total Volume (Barrels)") as "Total Volume (Barrels)").sort("Holiday", "Year").withColumn("Total Sales (mil)", format_number($"Total Sales (mil)", 2)).withColumn("Total Volume (Barrels)", regexp_replace(format_number($"Total Volume (Barrels)", 2), ",", ""))
        

        // Part 3 - Left anti join to get all days that are not one of the above specified holidays then group by year (and holiday to show 'None'), taking avg*3 for 3 day average
        val nonholidaysDF = dayGroupDF.join(holidayDaysDF, holidayDaysDF("Month") === dayGroupDF("Month") && holidayDaysDF("Day") === dayGroupDF("Day") ,"leftanti").withColumn("Holiday", lit("None"))
        val result3DF = nonholidaysDF.groupBy($"Holiday", $"Year").agg(avg($"Total Sales (mil)")*3 as "Total Sales - 3 Day AVG (mil)", avg($"Total Volume (Barrels)")*3 as "Total Volume - 3 Day AVG (Barrels)").sort("Year").withColumn("Total Sales - 3 Day AVG (mil)", format_number($"Total Sales - 3 Day AVG (mil)", 2)).withColumn("Total Volume - 3 Day AVG (Barrels)", regexp_replace(format_number($"Total Volume - 3 Day AVG (Barrels)", 2), ",", ""))
        

        // Show results and write to files
        // Part 1 Year, Month, Total Sales, Volume
        result1DF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question1Part1")
        result1DF.show(110)
        // Part 2 Holiday, Year, Total Sales over 3 days, Volume over 3 days
        result2DF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question1Part2")
        result2DF.show(50)
        // Part 3 Holiday(only 'None'),  Year, 3Day Avg Sales, 3Day Avg Volume
        result3DF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question1Part3")
        result3DF.show()
       
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