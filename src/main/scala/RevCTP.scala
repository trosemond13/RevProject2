import scala.Console.{BOLD, GREEN, RED, RESET, print, println}
import com.roundeights.hasher.Implicits._
import scala.io.StdIn

class RevCTP extends HiveDBC {
  val SUCCESS = 0
  val FAILURE = 1
  val ERROR: Int = -1
  var employees: Map[String, (String, Long, String, String, Boolean)] = getEmployees(false)
  val init: Boolean = if(employees.size == 0) false else true
  var email: String = ""
  var loggedIn: Boolean = false

  def initializeAdmin(): Unit = {
    updateEmployeeInfo(false, true, true, true, true, false)
  }
  def login(): Int = {
    var tries: Int = 3
    while(!loggedIn) {
      print(s"LOGIN MENU> Enter your email here -> ")
      email = StdIn.readLine().toLowerCase().trim
      print(s"LOGIN MENU> Enter your password here -> ")
      val password: String = StdIn.readLine().trim.sha256.hash
      if(employees.contains(email) && employees(email)._1 == password) {
        println(s"LOGIN MENU> Logged in as <${email.split('@')(0)}>")
        loggedIn = true
        return SUCCESS
      } else {
        tries = tries - 1
        if(tries>0) {
          println(s"LOGIN FAILURE> Incorrect email or password. $tries more tries left or the program will exit!")
        } else {
          println(s"SYSTEM> You typed in the incorrect email or password too many times. Exiting login page...")
          email = ""
          return FAILURE
        }
      }
    }
    FAILURE
  }

  override def createEmployee(employee_id: Long, first_name: String, last_name: String, email: String, password: String, admin: Boolean): Unit = {
    super.createEmployee(employee_id, first_name, last_name, email, password, admin)
  }

  def updateEmployeeInfo(terminate_bool: Boolean, first_name_bool: Boolean, last_name_bool: Boolean, email_bool: Boolean, password_bool: Boolean, admin_bool: Boolean): Unit = {
    val employee_id: Long = employees(email)._2
    var first_name: String = employees(email)._3
    var last_name = employees(email)._4
    var password = employees(email)._1

    if (!terminate_bool) {
      if (first_name_bool) {
        print("Enter the desired first name -> ")
        first_name = StdIn.readLine()
      }
      if (last_name_bool) {
        print("Enter the desired last name -> ")
        last_name = StdIn.readLine()
      }
      if (email_bool) {
        print("Enter the desired user name -> ")
        email = StdIn.readLine()
      }
      if (password_bool) {
        print("Enter the desired password-> ")
        password = StdIn.readLine()
      }
    }
    super.updateEmployeeInfo(terminate_bool, employee_id, first_name, last_name, email, password, admin_bool)
  }
  def checkEmail(e: String): Boolean = {
    val emailRegex = """^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$""".r
    e match {
      case null => false
      case e if e.trim.isEmpty => false
      case e if emailRegex.findFirstMatchIn(e).isDefined => true
      case _ => false
    }
  }

}
object Main {
  def main(args:Array[String]): Unit = {
    val rctp = new RevCTP()
    var currCommand = ""
    println(s"SYSTEM> Welcome to the Revature Covid Tracker Planning app. We appreciate your new subscription to our service.")
    //Initializes admin account
    if (!rctp.init) {
      println("FIRST TIME USE> Please sign in with the default login to initialize your employee database.")
      print(s"FIRST TIME USE> Enter your desired email -> ")
      val email = StdIn.readLine()
      print(s"FIRST TIME USE> Enter the desired password -> ")
      val password = StdIn.readLine().sha256.hash
      print(s"FIRST TIME USE> Enter your first name -> ")
      val first_name = StdIn.readLine()
      print(s"FIRST TIME USE> Enter your last name name -> ")
      val last_name = StdIn.readLine()
      rctp.createEmployee(employee_id = 8253, first_name, last_name, email, password, admin = true)
      print(s"SYSTEM> Admin account successfully made!")
    }

    do {
      println("Enter in one of the following option numbers or type ':quit' to exit the program.")
      println("1. ")
      println("1. ")
      println("1. ")
      println("1. ")
      println("1. ")
      println("1. ")
      println("1. ")

      currCommand = StdIn.readLine()
      if(currCommand == "1") {

      } else if(currCommand == "2") {

      } else if(currCommand == "3") {

      } else if(currCommand == "4") {

      } else if(currCommand == "5") {

      } else if(currCommand == "6") {

      } else {

      }
    } while(currCommand != ":quit")
  }
}