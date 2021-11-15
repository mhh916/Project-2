package analysis


import setup.SparkConnect
/* 
//Calling the function in 'Analyze'
val func = new CountyAgg 
func.groupByCounty()
 */

//My Additions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class expenseByLocal  (){
  val sc = new SparkConnect()

  def expenseByLocal(): Unit = {
    
    val df = sc.getDataFrame()
    df.createOrReplaceTempView("Liquor")
  

    //THIS WORKS
    println("Top 5 Counties That Spent the Most Money per Liter: ")                                                                                                     //Divisor                                                                                  
    sc.spark.sql("SELECT DISTINCT UPPER(County) AS COUNTY," +
      "ROUND(CAST(ROUND(SUM(CAST(`Sale (Dollars)` AS Double)) Over(PARTITION BY UPPER(County))/1,2) AS DECIMAL(10,2)) / CAST(ROUND(SUM(CAST(`Volume Sold (Liters)` AS Double)) Over(PARTITION BY UPPER(County))/1,2) AS DECIMAL(10,2)),2)" +
      "AS `Price/L(Dollars)` FROM Liquor WHERE UPPER(County) IS NOT NULL ORDER BY `Price/L(Dollars)` DESC LIMIT 5").show()

 


    println("Top 5 Zipcodes That Spent the Most Money per Liter: ")                                                                                                     //Divisor                                                                                  
    sc.spark.sql("SELECT DISTINCT `Zip Code` AS ZIPCODE," +
      "ROUND(CAST(ROUND(SUM(CAST(`Sale (Dollars)` AS Double)) Over(PARTITION BY `Zip Code`)/1,2) AS DECIMAL(10,2)) / CAST(ROUND(SUM(CAST(`Volume Sold (Liters)` AS Double)) Over(PARTITION BY `Zip Code`)/1,2) AS DECIMAL(10,2)),2)" +
      "AS `Price/L(Dollars)` FROM Liquor WHERE `Zip Code` IS NOT NULL ORDER BY `Price/L(Dollars)` DESC LIMIT 5").show()

       

    println("Top 5 Cities That Spent the Most Money per Liter: ")                                                                                                     //Divisor                                                                                  
    sc.spark.sql("SELECT DISTINCT UPPER(City) AS CITY," +
      "ROUND(CAST(ROUND(SUM(CAST(`Sale (Dollars)` AS Double)) Over(PARTITION BY UPPER(City))/1,2) AS DECIMAL(10,2)) / CAST(ROUND(SUM(CAST(`Volume Sold (Liters)` AS Double)) Over(PARTITION BY UPPER(City))/1,2) AS DECIMAL(10,2)),2)" +
      "AS `Price/L(Dollars)` FROM Liquor WHERE UPPER(City) IS NOT NULL ORDER BY `Price/L(Dollars)` DESC LIMIT 5").show()



  }
}