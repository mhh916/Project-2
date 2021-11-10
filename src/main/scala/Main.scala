import scala.io.StdIn._
import analysis.Analyze

object Main {

  def main(args: Array[String]): Unit = {
    var loop = true
    val a = new Analyze()
    
    // Venom ASCII Logo
    println("##     ## ######## ##    ##  #######  ##     ## \n##     ## ##       ###   ## ##     ## ###   ### \n##     ## ##       ####  ## ##     ## #### #### \n##     ## ######   ## ## ## ##     ## ## ### ## \n ##   ##  ##       ##  #### ##     ## ##     ## \n  ## ##   ##       ##   ### ##     ## ##     ## \n   ###    ######## ##    ##  #######  ##     ## ")
    // Main for loop for CLI Program
    do{
      println("Please select an option")
      println("1. Question 1\n2. Question 2\n3. Question 3\n4. Question 4\n5. Question 5\n6. Question 6\n7. Question 7\n8. Question 8\n9. Question 9\n0. Quit Application")
      try {
        
        val option = readInt()
        println()
        option match{
          case 1 => {
            a.q1()
          }
          case 2 => {
            a.q2()
          }
          case 3 => {
            a.q3()
          }
          case 4 => {
            a.q4()
          }
          case 5 => {
            a.q5()
          }
          case 6 => {
            a.q6()
          }
          case 7 => {
            a.q7()
          }
          case 8 => {
            a.q8()
          }
          case 9 => {
            a.q9()
          }
          case 0 => {
            loop = false
          }
      }
      }catch {
        case e: MatchError => println("Please pick a number between 0~9\n")
        case e: NumberFormatException => println("\nPlease enter a number\n") 
      }
      
    } while(loop) 
    println("Thank you")
  }

}