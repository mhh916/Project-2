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


class CountyAgg  (){
  val sc = new SparkConnect()

  def groupByCounty(): Unit = {
    
    val df = sc.getDataFrame()
    df.createOrReplaceTempView("Liquor")
  

    //THIS WORKS
    println("Top 5 Counties That Spent the Most Money on Liquor: ")
    sc.spark.sql("SELECT DISTINCT UPPER(County) AS COUNTY, CAST(ROUND(SUM(CAST(`Sale (Dollars)` AS Double)) Over(PARTITION BY UPPER(County))/1000000,2) AS DECIMAL(10,2)) AS `Total Sales(mil)` FROM Liquor ORDER BY `Total Sales(mil)` DESC LIMIT 5").show()


   
  }
}