import scala.io.StdIn._

object Main {

  def main(args: Array[String]): Unit = {
    var loop = true
    // Venom ASCII Logo
    println("##     ## ######## ##    ##  #######  ##     ## \n##     ## ##       ###   ## ##     ## ###   ### \n##     ## ##       ####  ## ##     ## #### #### \n##     ## ######   ## ## ## ##     ## ## ### ## \n ##   ##  ##       ##  #### ##     ## ##     ## \n  ## ##   ##       ##   ### ##     ## ##     ## \n   ###    ######## ##    ##  #######  ##     ## ")
    // Main for loop for CLI Program
    do{
      
      println("Please select an option")
      println("1. Nothing\n2. Quit Application")
      try {
      val option = readInt()
      println()
      option match{
        case 1 => {
           
        }
        case 2 => {
          loop = false
        }
      }
      }catch {
        case e: MatchError => println("Please pick a number between 1~2\n")
        case e: NumberFormatException => println("\nPlease enter a number\n") 
      }
      
    } while(loop) 
    println("Thank you")
  }

}