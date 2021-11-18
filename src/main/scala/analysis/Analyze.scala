package analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import setup.SparkConnect
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

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
      val func = new CountyAgg 
      func.groupByCounty()
    }

    def q3(): Unit = {
      val func = new expenseByLocal
      func.expenseByLocal() 
    }

    def q4(): Unit = {
        val p213 = new analysisP213(sc)
        p213.q4()
    }

    def q5(): Unit = {
        sc.getDataFrame.createOrReplaceTempView("liquor")
        
        //Which vendor had the most revenue liquor sales? What was their best-selling category?
        println("Getting vendor with most Revenue, Limit 3")
        val q5 = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`VendorName`) AS Vendor, ROUND(SUM(CAST(`StateBottleCost` AS Double)) OVER(PARTITION BY UPPER(`VendorName`))/1000000,2) AS `Revenue(mil)`  FROM liquor ORDER BY `Revenue(mil)` DESC LIMIT 3")
        q5.show(false)
        // Grabs the 3 vendor names from dataframe
        val vendor1 = q5.collect()(0)(0)
        val vendor2 = q5.collect()(1)(0)
        val vendor3 = q5.collect()(2)(0)

        // Runs queries with provided vendor names.
        println("Getting "+ vendor1 + " Most Popular Category, Limit 3")
        val q5a = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`CategoryName`) AS Category, ROUND((SUM(`BottlesSold`) OVER(PARTITION BY  UPPER(`CategoryName`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`VendorName`) = " + "'" + vendor1 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5a.show(false)
        println("Getting "+ vendor2 + " Most Popular Category, Limit 3")
        val q5b = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`CategoryName`) AS Category, ROUND((SUM(`BottlesSold`) OVER(PARTITION BY  UPPER(`CategoryName`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`VendorName`) = " + "'" + vendor2 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5b.show(false)
        println("Getting "+ vendor3 + " Most Popular Category, Limit 3")
        val q5c = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`CategoryName`) AS Category, ROUND((SUM(`BottlesSold`) OVER(PARTITION BY  UPPER(`CategoryName`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`VendorName`) = " + "'" + vendor3 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5c.show(false)
    }

    def q6(): Unit = {
        sc.getDataFrame.createOrReplaceTempView("liquor")

        // Part 1/3
        // Show most expensive liquors per liter
        println("Most expensive liquors by volume:")
        val mostExpensiveDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`Sale (Dollars)` / `Volume Sold (Liters)`) AS DECIMAL(10,2))" +
            "AS `Price/Liter (Dollars)` FROM liquor GROUP BY `Item Description` ORDER BY `Price/Liter (Dollars)` DESC LIMIT 20")
        mostExpensiveDF.show(false)

        // Show least expensive liquors per liter
        println("Least expensive liquors by volume:")
        val leastExpensiveDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`Sale (Dollars)` / `Volume Sold (Liters)`) AS DECIMAL(10,2))" +
            "AS `Price/Liter (Dollars)` FROM liquor GROUP BY `Item Description` HAVING `Price/Liter (Dollars)` > 0 ORDER BY `Price/Liter (Dollars)` ASC LIMIT 10")
        leastExpensiveDF.show(false)

        // Part 2/3
        // Show liquors with highest volume sold
        println("Most popular liquor by volume sold:")
        val mostPopularVolumeDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(SUM(`Volume Sold (Liters)`) / 1000000 AS DECIMAL(5,2)) AS `Liters Sold (mil)`" +
          "FROM liquor GROUP BY `Item Description` ORDER BY `Liters Sold (mil)` DESC LIMIT 10")
        mostPopularVolumeDF.show(false)

        // Part 3/3
        // Show liquors sold by total dollars spent
        println("Most popular liquor by dollars spent:")
        val mostPopularDollarsDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(SUM(`Sale (Dollars)`) / 1000000 AS DECIMAL(5,2)) AS `Dollars (mil)`" +
          "FROM liquor GROUP BY `Item Description` ORDER BY `Dollars (mil)` DESC LIMIT 10")
        mostPopularDollarsDF.show(false)
    }

    def q7(): Unit = {
        sc.getDataFrame.createOrReplaceTempView("liquor")

        // Show the top 5 most profitbale liquors by their average profit (bottle retail cost - bottle state cost)
        println("5 most profitable liquors (retail cost - state cost):")
        val highestAvgDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`State Bottle Retail` - `State Bottle Cost`) AS DECIMAL(10,2))" +
                                    " AS `Average Profit (Dollars)` FROM liquor GROUP BY `Item Description` ORDER BY `Average Profit (DOLLARS)` DESC LIMIT 5")
        highestAvgDF.show(false)

        // Show the top 5 least profitbale liquors by their average profit (bottle retail cost - bottle state cost)
        println("5 least profitable liquors (retail cost - state cost):")
        val lowestAvgDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`State Bottle Retail` - `State Bottle Cost`) AS DECIMAL(10,2))" +
                                    " AS `Average Profit (Dollars)` FROM liquor GROUP BY `Item Description` ORDER BY `Average Profit (Dollars)` ASC LIMIT 5")
        lowestAvgDF.show(false)
    }

    def q8(): Unit = {
        val spark = sc.getSparkSession()
        import spark.implicits._
        try{
            val q1DF = spark.read.option("header", true).option("inferSchema", true).csv("/user/maria_dev/Project2/Question1Part1/part*.csv")
            val preCovidDF = q1DF.filter("Year <= 2019 OR Year == 2020 AND Month < 3")
            val postCovidDF = q1DF.filter("Year = 2020 AND Month >= 3")
            preCovidDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question8PreCovid")
            preCovidDF.show(100)
            postCovidDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question8PostCovid")
            postCovidDF.show()
            println("Result obtained using result file from question1, part1.")
        }
        catch{
            case e: Throwable => {
                val spark = sc.getSparkSession()
                val inputDF = sc.getDataFrame()
                //Split Date into separate columns for year, month, and day. Also cast Sales and Volume to Double.
                val modifiedDF = inputDF.withColumn("Year", year(to_date(col("Date"),"MM/dd/yyyy"))).withColumn("Month", month(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Day", dayofmonth(to_date(col("Date"), "MM/dd/yyyy"))).withColumn("Sales", col("Sale (Dollars)").cast(DoubleType)).withColumn("Volume", col("Volume Sold (Gallons)").cast(DoubleType))
                //Result for question 1 part 1, group by year+month, sum sales and volume. Sales in millions of dollars, volume in barrels (53gallons/barrel)
                val q1DF = modifiedDF.groupBy($"Year", $"Month").agg(sum($"Sales")/1000000 as "Total Sales (mil)", sum($"Volume")/53 as "Total Volume (Barrels)").sort(col("Year"), col("Month")).withColumn("Total Sales (mil)", format_number($"Total Sales (mil)", 2)).withColumn("Total Volume (Barrels)", regexp_replace(format_number($"Total Volume (Barrels)", 2), ",", ""))
                val preCovidDF = q1DF.filter("Year <= 2019 OR Year == 2020 AND Month < 3")
                val postCovidDF = q1DF.filter("Year = 2020 AND Month >= 3")
                preCovidDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question8PreCovid")
                preCovidDF.show(100)
                postCovidDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("/user/maria_dev/Project2/Question8PostCovid")
                postCovidDF.show()
                println("Result file from question1, part1 not found, result created from scratch.")
            }
        }
    }

    def q9(): Unit = {
        println("Preparing DF VIEW")
        val df1 = sc.getDataFrame()
        val df2 = df1.withColumn("Day of Week", date_format(to_date(col("Date"),"MM/dd/yyyy"),"E"))
        val df3 = df2.groupBy("Day of Week").agg(sum("Volume Sold (liters)").as("total vol(L) sold"))
        val df4 = df3.orderBy(col("total vol(L) sold").desc)
        //accumulating volume of liquor sold on each day
        println("Printing...")
        println("Liquor volume sold on days of week")
        df4.withColumn("Total Volume (Barrels)", (df4("total vol(L) sold")/240.9428).cast(DecimalType(20,2))).select("Day of Week","Total Volume (Barrels)").show(7,false)    
    }
}