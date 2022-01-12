import scala.Console.{BOLD, GREEN, RED, RESET, print, println}
import com.roundeights.hasher.Implicits._
import com.tools.Router._
import com.tools.HiveDBC
import scala.io.StdIn

class RevCTP extends HiveDBC {
  val SUCCESS: Int = 0
  val FAILURE: Int = 1
  val ERROR: Int = -1
  var employees: Map[String, (String, Long, String, String, Boolean)] = getEmployees(false)
  val init: Boolean = if(employees.size == 0) false else true
  var email: String = ""
  var loggedIn: Boolean = false

  protected def initializeAdmin(): Unit = {
    println("FIRST TIME USE> Please sign in with the default login to initialize your employee database.")
    print("FIRST TIME USE> Enter your desired email -> ")
    val email = StdIn.readLine()
    print("FIRST TIME USE> Enter the desired password -> ")
    val password = StdIn.readLine().sha256.hash
    print("FIRST TIME USE> Enter your first name -> ")
    val first_name = StdIn.readLine()
    print("FIRST TIME USE> Enter your last name name -> ")
    val last_name = StdIn.readLine()

    createEmployee(employee_id = 8253, first_name, last_name, email, password, admin = true)
    employees += email -> (password, 8253, first_name, last_name, true)
    print("SYSTEM> Admin account successfully initialized!")
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
  def logout(): Unit = {
    if(loggedIn) {
      email = ""
      loggedIn = false
      println(s"SYSTEM> User with email <$email> has been logged out.")
    } else {
      println("No user is logged in. Logout failed.")
    }
  }
  override def createEmployee(employee_id: Long, first_name: String, last_name: String, email: String, password: String, admin: Boolean): Unit = {
    if(employees.contains(email)) {
      println("SYSTEM> Email is already linked to an account.")
    } else {
      try {
        super.createEmployee(employee_id, first_name, last_name, email, password, admin)
        println(s"Account with email <$email> has been successfully created.")
      } catch {
        case _ => println(s"Account with email <$email> has not been created. Try again later.")
      }
    }
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
        password = StdIn.readLine().sha256.hash
      }
      employees += email -> (password, employee_id, first_name, last_name, admin_bool)
    } else {
      employees -= email
    }
    super.updateEmployeeInfo(terminate_bool, employee_id, first_name, last_name, email, password, admin_bool)
  }
  def checkEmail(e: String): Boolean = {
    val emailRegex = """^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])+(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$""".r
    e match {
      case null => false
      case e if e.trim.isEmpty => false
      case e if emailRegex.findFirstMatchIn(e).isDefined => true
      case _ => false
    }
  }
}

object Main extends RevCTP {
  def main(args: Array[String]): Unit = {
    val rctp = new RevCTP()
    var currCommand = ""

    println(s"SYSTEM> Welcome to the Revature Covid Tracker Planning app. We appreciate your subscription to our service.")

    if (!rctp.init) { //Initializes admin account if there are no users in the database
      initializeAdmin()
    }

    do {
      println(
        """Please Select A Menu Option
          |
          |1.) Recovery Rates Data
          |2.) Mortality Rates Data
          |3.) Infection Rates Data
          |4.) Logout And Exit""".stripMargin)
      currCommand = StdIn.readLine()

      // Router Functions Based On Input
      val router = currCommand match {
        case "1" => recovery_data_route(rctp.spark)
        case "2" => mortality_data_route()
        case "3" => infection_data_route()
        case "4" => println("Goodbye!")
        case _ => println("Invalid Option")
      }

      /*println("Enter in one of the following option numbers or type ':quit' to exit the program.")
      println("Login/Logout")
      println("1. ")
      println("1. ")
      println("1. ")
      println("1. ")
      println("Settings")
      println(":quit")
      print("> ")*/
      //currCommand = StdIn.readLine()
    } while(currCommand != "4")
  }
}

      /*if(currCommand == "Login/Logout") {
        if(loggedIn)
          logout()
        else
          login()
      } else if(currCommand == "2") {
        do {
          print("> ")
          val email = StdIn.readLine()
          println(checkEmail(email))
        } while(email != ":quit")
      } else if(currCommand == "3") {

      } else if(currCommand == ":quit") {
        println("Quitting application!")
      } else if(currCommand == "Settings") { //Will be used to view and/or update user information

      } else {
        println("SYSTEM> Invalid Choice!")
      }
    } while(currCommand != ":quit")
  }
}*/