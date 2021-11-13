package analysis

import setup.SparkConnect
import org.apache.spark.sql._

class Analyze   (){
    val sc = new SparkConnect()

    def q1(): Unit = {
     
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
        
    }

    def q5(): Unit = {
        sc.getDataFrame.createOrReplaceTempView("liquor")
        
        //Which vendor had the most revenue liquor sales? What was their best-selling category?
        println("Getting vendor with most Revenue, Limit 3")
        val q5 = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`Vendor Name`) AS Vendor, ROUND(SUM(CAST(`State Bottle Cost` AS Double)) OVER(PARTITION BY UPPER(`Vendor Name`))/1000000,2) AS `Revenue(mil)`  FROM liquor ORDER BY `Revenue(mil)` DESC LIMIT 3")
        q5.show(false)
        // Grabs the 3 vendor names from dataframe
        val vendor1 = q5.collect()(0)(0)
        val vendor2 = q5.collect()(1)(0)
        val vendor3 = q5.collect()(2)(0)

        // Runs queries with provided vendor names.
        println("Getting "+ vendor1 + " Most Popular Category, Limit 3")
        val q5a = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`Category Name`) AS Category, ROUND((SUM(`Bottles Sold`) OVER(PARTITION BY  UPPER(`Category Name`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`Vendor Name`) = " + "'" + vendor1 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5a.show(false)
        println("Getting "+ vendor2 + " Most Popular Category, Limit 3")
        val q5b = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`Category Name`) AS Category, ROUND((SUM(`Bottles Sold`) OVER(PARTITION BY  UPPER(`Category Name`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`Vendor Name`) = " + "'" + vendor2 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5b.show(false)
        println("Getting "+ vendor3 + " Most Popular Category, Limit 3")
        val q5c = sc.getSparkSession().sql("SELECT DISTINCT UPPER(`Category Name`) AS Category, ROUND((SUM(`Bottles Sold`) OVER(PARTITION BY  UPPER(`Category Name`))/1000000), 2) AS  `Sold(mil)` FROM liquor WHERE UPPER(`Vendor Name`) = " + "'" + vendor3 + "' ORDER BY `Sold(mil)` DESC LIMIT 3")
        q5c.show(false)
    }

    def q6(): Unit = {
        
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