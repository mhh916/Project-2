package analysis

import setup.SparkConnect
import org.apache.spark.sql._

class Analyze   (){
    val sc = new SparkConnect()

    def q1(): Unit = {

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
        sc.getDataFrame.createOrReplaceTempView("liquor")

        // Show most expensive liquors per liter
        println("Most expensive liquors by volume:")
        val mostExpensiveDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`Sale (Dollars)` / `Volume Sold (Liters)`) AS DECIMAL(10,2)) AS `Price/Liter (Dollars)`" +
          "FROM liquor GROUP BY `Item Description` ORDER BY `Price/Liter (Dollars)` DESC LIMIT 20")
        mostExpensiveDF.show(false)

        // Show least expensive liquors per liter
        println("Least expensive liquors by volume:")
        val leastExpensiveDF = sc.getSparkSession.sql("SELECT `Item Description`, CAST(AVG(`Sale (Dollars)` / `Volume Sold (Liters)`) AS DECIMAL(10,2)) AS `Price/Liter (Dollars)`" +
          "FROM liquor GROUP BY `Item Description` HAVING `Price/Liter (Dollars)` > 0 ORDER BY `Price/Liter (Dollars)` ASC LIMIT 10")
        leastExpensiveDF.show(false)
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
        
    }

    def q9(): Unit = {
        
    }
}