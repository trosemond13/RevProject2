import Main.{email, getAdminStatus}

import scala.Console.{BLUE, BOLD, RESET, print, println}
import com.roundeights.hasher.Implicits._
import com.tools.HiveDBC

import scala.io.StdIn

class RevCTP extends HiveDBC {
  val SUCCESS: Int = 0
  val FAILURE: Int = 1
  val ERROR: Int = -1
  var employees: Map[String, (String, Long, String, String, Boolean)] = getEmployees(false)
  val init: Boolean = if(employees.isEmpty) false else true
  var email: String = ""
  var loggedIn: Boolean = false

  protected def initializeAdmin(): Unit = {
    println("FIRST TIME USE> Please sign in with the default login to initialize your employee database.")

    print("FIRST TIME USE> Enter your desired email -> ")
    var email = StdIn.readLine()
    while (!checkEmail(email)) {
      print("FIRST TIME USE> Invalid email format (Ex. _@_._). Try again -> ")
      email = StdIn.readLine()
    }

    print("FIRST TIME USE> Enter the desired password -> ")
    var password = StdIn.readLine().sha256.hash
    print("FIRST TIME USE> Retype in the desired password to confirm. -> ")
    var conf_password = StdIn.readLine().sha256.hash
    while(password != conf_password) {
      print("FIRST TIME USE> Passwords do not match. Try entering in your password again -> ")
      password = StdIn.readLine().sha256.hash
      print("FIRST TIME USE> Retype in the desired password to confirm. -> ")
      conf_password = StdIn.readLine().sha256.hash
    }

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
      loggedIn = false
      println(s"SYSTEM> User with email <$email> has been logged out.")
      email = ""
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
        case _: Throwable => println(s"Account with email <$email> has not been created. Try again later.")
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
  def a(): Unit = {
    println(
      """Please Select A Menu Option
        |
        |1.) Update Account Information
        |2.) Search Employee
               """.stripMargin)
   var currCommand = StdIn.readLine()

    // Router Functions Based On Input
    val router = currCommand match {
      case "1" =>
      case "2" =>
      case "3" =>
      case "4" => println("Goodbye!")
      case _ => println("Invalid Option")
    }
  }
  def getAdminStatus(email: String): Boolean = {
    if(!employees.contains(email))
      false
    else
      employees(email)._5
  }
  def quit: Unit = {
    print("Exiting in 5 seconds. Syncing System.")
    println("Exiting[....(5)]")
    println("Exiting[...(4).]")
    println("Exiting[..(3).]")
    println("Exiting[.(2)...]")
    println("Exiting[(1)....]")
    println("Exiting Complete!")
  }
  def startRCTP(): Unit = {
    var break = false
    do {
      clearScreen
      println(
        """RCTP MENU> 1.) Recovery
          |RCTP MENU> 2.) Death
          |RCTP MENU> 3.) Confirmed
          |RCTP MENU> 4.) Back to Main Menu""".stripMargin)

      if(getAdminStatus(email))
        print(s"$BOLD$BLUE${email.split('@')(0).capitalize}$RESET> ")
      else
        print(s"${email.split('@')(0).capitalize}> ")

      StdIn.readLine() match {
        case "1" => println("option 1")
        case "2" => println("option 2")
        case "3" => println("option 3")
        case "4" => println("option 4")
        case "quit" => break = true
        case _ => println("Invalid Option. Enter a valid number option.")
      }
      clearScreen
    } while (!break)
  }
  def settings(str: String) = {
    var break = false
    clearScreen
    do {
      println(
        """SETTINGS MENU> 1.) Update Account Info.
          |SETTINGS MENU> 2.) Check Version
          |SETTINGS MENU> 3.) Back to Main Menu""".stripMargin)

      if(getAdminStatus(email))
        print(s"$BOLD$BLUE${email.split('@')(0).capitalize}$RESET> ")
      else
        print(s"${email.split('@')(0).capitalize}> ")

      StdIn.readLine() match {
        case "1" => println("option 1")
        case "quit" => break = true
        case _ => println("Invalid Option. Enter a valid number option.")
      }
      clearScreen
    } while (!break)
  }
  def clearScreen: Unit = {
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
    println()
  }
}
object Main extends RevCTP {
  def main(args:Array[String]): Unit = {
    val rctp = new RevCTP()
    var currCommand = ""
    var break = false

    println(s"SYSTEM> Welcome to the Revature Covid Tracker Planning app. We appreciate your subscription to our service.")

    if (!rctp.init) { //Initializes admin account if there are no users in the database
      initializeAdmin()
    }
    do {
      println("MAIN MENU> Please select one of the following menu options.")
      if(!loggedIn) {
        println(
          """|MAIN MENU> 1.) Login
            |MAIN MENU> 2.) Quit""".stripMargin)
      } else {
        println(
          """MAIN MENU> 1.) Logout
            |MAIN MENU> 2.) StartRCTP
            |MAIN MENU> 3.) Settings
            |MAIN MENU> 4.) Quit Program""".stripMargin)
      }
      if(getAdminStatus(email))
        print(s"$BOLD$BLUE${email.split('@')(0).capitalize}$RESET> ")
      else
        print(s"${email.split('@')(0).capitalize}> ")

      currCommand = StdIn.readLine()
      currCommand match {
        case "1" if !loggedIn => login()
        case "1" if loggedIn => logout()
        case "2" if !loggedIn => quit; break = true
        case "2" => startRCTP()
        case "3" if loggedIn && getAdminStatus(email) => settings(str = "admin")
        case "3" if loggedIn => settings(str = "basic")
        case "4" if loggedIn => quit; break = true
        case _ => println("Invalid Option. Enter a valid number option."); currCommand += "!"
      }
      clearScreen
    } while(!break)
  }
}