package com

import com.roundeights.hasher.Implicits._
import com.tools.HiveDBC
import com.tools.Router._

import scala.Console.{print, println}
import scala.io.AnsiColor._
import scala.io.StdIn

class RevCTP extends HiveDBC {
  val SUCCESS: Int = 0
  val FAILURE: Int = 1
  val ERROR: Int = -1
  var employees: Map[String, (String, Long, String, String, Boolean)] = getEmployees(false)
  val init: Boolean = if(employees.isEmpty) false else true
  var email: String = ""
  var loggedIn: Boolean = false

  protected def checkEmail(e: String): Boolean = {
    val emailRegex = """^[a-zA-Z0-9.{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])*(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$""".r
    e match {
      case null => false
      case e if e.trim.isEmpty => false
      case e if emailRegex.findFirstMatchIn(e).isDefined => true
      case _ => false
    }
  }

  def clearScreen(): Unit = {
    println("============================================================================================================================")
    println("-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_")
    println("============================================================================================================================")
  }

  protected def createEmployee(): Unit = {
    var tries = 2
    println("MAIN/SETTINGS/CREATE ACCOUNT")
    print("--> Enter the email of the account you would like to create -> ")
    var email = StdIn.readLine()
    while((!checkEmail(email) || employees.contains(email)) && tries > 0) {
      if(!checkEmail(email)) {
        print("SYSTEM> Invalid email format (Ex. _@_._). Try again -> ")
      }
      if(employees.contains(email)) {
        print("SYSTEM> Email is unavailable. Try another email -> ")
      }
      email = StdIn.readLine()
      tries = tries - 1
      if(tries == 0) {
        println("SYSTEM> You have entered invalid input too many times. Returning to Settings menu.")
      }
    }
    if(tries > 0) {
      print("--> Enter a password for the account. -> ")
      var password = StdIn.readLine().sha256.hash
      print("--> Re-enter password to confirm. -> ")
      var conf_password = StdIn.readLine().sha256.hash
      while (password != conf_password) {
        print("SYSTEM> Passwords do not match. Try entering in your password again -> ")
        password = StdIn.readLine().sha256.hash
        print("SYSTEM> Retype in the desired password to confirm. -> ")
        conf_password = StdIn.readLine().sha256.hash
      }
      print("--> Enter the user's first name -> ")
      val first_name = StdIn.readLine()
      print("--> Enter the user's last name -> ")
      val last_name = StdIn.readLine()
      try {
        val employee_id = generateNewEmployeeID
        createEmployee(employee_id, first_name, last_name, email, password, admin = false)
        employees += email -> (password, employee_id, first_name, last_name, false)
        println(s"SYSTEM> <$email>'s account has successfully been created.")
      } catch {
        case _: Throwable => println(s"SYSTEM> <$email>'s account has not been created. Please try again.")
      }
    }
  }

  protected def getAdminStatus(email: String): Boolean = {
    if(!employees.contains(email))
      false
    else
      employees(email)._5
  }

  protected def initializeAdmin(): Unit = {
    println("FIRST TIME USE> Please sign in with the default login to initialize your employee database.")

    print("FIRST TIME USE> Enter your desired email -> ")
    var email = StdIn.readLine()
    while (!checkEmail(email)) {
      print(s"${RED}FIRST TIME USE> Invalid email format (Ex. _@_._).$RESET Try again -> ")
      email = StdIn.readLine()
    }

    print("FIRST TIME USE> Enter the desired password -> ")
    var password = StdIn.readLine().sha256.hash
    print("FIRST TIME USE> Retype in the desired password to confirm. -> ")
    var conf_password = StdIn.readLine().sha256.hash
    while(password != conf_password) {
      print(s"${RED}FIRST TIME USE> Passwords do not match.$RESET Try entering in your password again -> ")
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
    clearScreen()
  }

  protected def login(): Int = {
    var tries: Int = 3
    clearScreen()
    while(!loggedIn) {
      println("MAIN/LOGIN")
      print(s"-> Enter your email here -> ")
      email = StdIn.readLine().toLowerCase().trim
      print(s"-> Enter your password here -> ")
      val password: String = StdIn.readLine().trim.sha256.hash
      if(employees.contains(email) && employees(email)._1 == password) {
        println(s"-> ${GREEN}Logged in as <$RESET${email.split('@')(0)}$GREEN>$RESET")
        loggedIn = true
        return SUCCESS
      } else {
        tries = tries - 1
        if(tries>0) {
          println(s"${RED}LOGIN FAILURE> Incorrect email or password.$RESET $tries more tries left or the program will return to the main menu!")
        } else {
          println(s"${RED}SYSTEM> You typed in the incorrect email or password too many times.$RESET Returning back to main menu...")
          email = ""
          return FAILURE
        }
      }
    }
    FAILURE
  }

  protected def logout(): Unit = {
    if(loggedIn) {
      loggedIn = false
      println(s"${GREEN}SYSTEM> User with email <$email> has been logged out.$RESET")
      email = ""
    } else {
      println("No user is logged in. Logout failed.")
    }
  }

  def promptMessage(): Unit = {
    if(getAdminStatus(email))
      print(s"$BOLD$BLUE${email.split('@')(0).capitalize}$RESET> ")
    else
      print(s"${email.split('@')(0).capitalize}> ")
  }

  protected def quit(): Unit = {
    clearScreen()
    println("Exiting in 5 seconds. Syncing System.")
    println("Exiting[....(5)]")
    Thread.sleep(1000)
    println("Exiting[...(4).]")
    Thread.sleep(1000)
    println("Exiting[..(3)..]")
    Thread.sleep(1000)
    println("Exiting[.(2)...]")
    Thread.sleep(1000)
    println("Exiting[(1)....]")
    Thread.sleep(1000)
    println("Exiting Complete!")
  }

  protected def startRCTP(): Unit = {
    var break = false
    do {
      clearScreen()
      println(s"${UNDERLINED}MAIN/START RCTP: Please select one of the following menu options.$RESET\n")
      println(
        """-> 1.) Recoveries
          |-> 2.) Deaths
          |-> 3.) Confirmed Infections
          |-> 4.) Back to Main Menu""".stripMargin)

      promptMessage()

      StdIn.readLine() match {
        case "1" => recovery_data_route()
        case "2" => mortality_data_route()
        case "3" => infection_data_route()
        case "4" => break = true
        case "quit" => break = true
        case _ => println("Invalid Option. Enter a valid number option.")
      }
    } while (!break)
  }

  protected def settings(status: String): Unit = {
    var break = false
    var first_name_change = false
    var last_name_change = false
    var email_change = false
    var password_change = false

    clearScreen()
    do {
      var deleted_employees = getEmployees(true)
      println(s"${UNDERLINED}MAIN/SETTINGS: Please select one of the following menu options.$RESET\n")
      if(status == "admin") {
        println(
          """-> 1.) Update Account Info.
            |-> 2.) Revoke/Reinstate Account Access.
            |-> 3.) Grant/Revoke Admin Access.
            |-> 4.) Create a basic account.
            |-> 5.) Return to Main Menu""".stripMargin)
      } else {
        println(
          """-> 1.) Update Account Info.
            |-> 2.) Return to Main Menu.""".stripMargin)
      }
      promptMessage()
      val currOption = StdIn.readLine()
      if (currOption == "1" ) {
        clearScreen()
        print("UPDATE INFO> Would you like to update your first name? (y/_) -> ")
        first_name_change = if (StdIn.readLine().toLowerCase == "y") true else false
        print("UPDATE INFO> Would you like to update your last name? (y/_) -> ")
        last_name_change = if (StdIn.readLine().toLowerCase == "y") true else false
        print("UPDATE INFO> Would you like to update your email? (y/_) -> ")
        email_change = if (StdIn.readLine().toLowerCase == "y") true else false
        print("UPDATE INFO> Would you like to update your password? (y/_) -> ")
        password_change = if (StdIn.readLine().toLowerCase == "y") true else false
        updateEmployeeInfo(first_name_change, last_name_change, email_change, password_change)
        clearScreen()
      } else if (currOption == "2" && status != "admin") {
        break = true
      } else if (currOption == "2") {
        clearScreen()
        print("UPDATE ACCESS> Enter the email of the account you would like to revoke/reinstate app access -> ")
        var email_in = StdIn.readLine()
        var tries = 2
        while ((email_in == email || (!employees.contains(email_in) && !deleted_employees.contains(email_in))) && tries > 0) {
          if(email_in == email) {
            print(s"${RED}SYSTEM> Permission denied.$RESET You cannot revoke your own account access. Try another email ($tries more tries left) -> ")
          }
          if (!employees.contains(email_in)) {
            print(s"${RED}SYSTEM> Permission denied. Employee does not exist.$RESET Try again ($tries more tries left) -> ")
          }
          email_in = StdIn.readLine()
          if(tries == 0) {
            println(s"${RED}SYSTEM> You have entered invalid input too many times.$RESET Returning to Settings menu.")
          }
          tries = tries - 1
        }
        if(deleted_employees.contains(email_in)) {
          val employee_id = deleted_employees(email_in)._2
          val first_name = deleted_employees(email_in)._3
          val last_name = deleted_employees(email_in)._4
          val password = deleted_employees(email_in)._1
          updateEmployeeInfo(terminate = false, employee_id, first_name, last_name, email_in, password, admin = false)
          employees += email_in -> (password, employee_id, first_name, last_name, false)
          deleted_employees -= email_in
          println(s"$GREEN${BOLD}SYSTEM> Permission Granted. <$email_in>'s account access has been reinstated!$RESET")
        } else if(employees.contains(email_in)) {
          val employee_id = employees(email_in)._2
          val first_name = employees(email_in)._3
          val last_name = employees(email_in)._4
          val password = employees(email_in)._1
          updateEmployeeInfo(terminate = true, employee_id, first_name, last_name, email_in, password, admin = false)
          employees -= email_in
          println(s"$GREEN${BOLD}SYSTEM> Permission Granted. <$email_in>'s account access has been revoked!$RESET")
        }
        clearScreen()
      } else if(currOption == "3" && status == "admin") {
        clearScreen()
        print("UPDATE ADMIN ACCESS> Enter the email of the account you would like to grant/revoke admin access -> ")
        var email_in = StdIn.readLine()
        var tries = 2
        while ((email_in == email || (!employees.contains(email_in) && !deleted_employees.contains(email_in))) && tries > 0) {
          if (email_in == email) {
            print(s"${RED}SYSTEM> Permission denied.$RESET You cannot revoke your own admin access. Try another email ($tries more tries left) -> ")
          }
          if (!employees.contains(email_in)) {
            print(s"${RED}SYSTEM> Permission denied.$RESET Employee does not exist. Try again ($tries more tries left) -> ")
          }
          email_in = StdIn.readLine()
          tries = tries - 1
          if (tries == 0) {
            println(s"${RED}SYSTEM> You have entered invalid input too many times.$RESET Returning to Settings menu.")
          }
        }
        if(employees.contains(email_in)) {
          val employee_id = employees(email_in)._2
          val first_name = employees(email_in)._3
          val last_name = employees(email_in)._4
          val password = employees(email_in)._1
          if (employees(email_in)._5) {
            updateEmployeeInfo(terminate = false, employee_id, first_name, last_name, email_in, password, admin = false)
            employees += email_in -> (password, employee_id, first_name, last_name, false)
            println(s"$BOLD${GREEN}SYSTEM> Permission Granted. <$email>'s admin privileges has been revoked.$RESET")
          } else {
            updateEmployeeInfo(terminate = false, employee_id, first_name, last_name, email_in, password, admin = true)
            employees += email_in -> (password, employee_id, first_name, last_name, true)
            println(s"$BOLD${GREEN}SYSTEM> Permission Granted. <$email>'s account has gained admin privileges.$RESET")
          }
        }
        clearScreen()
      } else if (currOption == "4" && status == "admin") {
        clearScreen()
        createEmployee()
        clearScreen()
      } else if(currOption == "5" && status == "admin") {
        break = true
      } else {
        println("SYSTEM> Invalid Option. Enter a valid number option.")
        clearScreen()
      }
    } while(!break)
  }

  protected def updateEmployeeInfo(first_name_bool: Boolean, last_name_bool: Boolean, email_bool: Boolean, password_bool: Boolean): Unit = {
    val employee_id: Long = employees(email)._2
    var first_name: String = employees(email)._3
    var last_name = employees(email)._4
    var password = employees(email)._1
    val admin = employees(email)._5

    println("MAIN/SETTINGS/UPDATE\n")
    if (first_name_bool) {
      print("SYSTEM> Enter the desired first name -> ")
      first_name = StdIn.readLine()
    }
    if (last_name_bool) {
      print("SYSTEM> Enter the desired last name -> ")
      last_name = StdIn.readLine()
    }
    if (email_bool) {
      print("SYSTEM> Enter the desired user name -> ")
      email = StdIn.readLine()
    }
    if (password_bool) {
      print("--> Enter the desired password -> ")
      password = StdIn.readLine().sha256.hash
    }
    if(first_name_bool || last_name_bool || email_bool || password_bool) {
      employees += email -> (password, employee_id, first_name, last_name, admin)
      super.updateEmployeeInfo(terminate = false, employee_id, first_name, last_name, email, password, admin)
      println(s"${GREEN}SYSTEM> Your account information has been successfully updated.$RESET")
    } else {
      println(s"${GREEN}SYSTEM> You have not made any changes to your account.$RESET")
    }
  }
}

object Main extends RevCTP {
  def main(args:Array[String]): Unit = {
    val rctp = new RevCTP()
    var currCommand = ""
    var break = false

    println(s"SYSTEM> Welcome to the Revature Covid Tracker Planner App. We appreciate your subscription to our service.")
    dbCon = rctp.spark

    if (!rctp.init) { //Initializes admin account if there are no users in the database
      initializeAdmin()
    }
    do {
      println(s"${UNDERLINED}MAIN MENU: Please select one of the following menu options.$RESET\n")
      if(!loggedIn) {
        println(
          """|> 1.) Login
             |> 2.) Exit""".stripMargin)
      } else {
        println(
          """> 1.) Logout
            |> 2.) StartRCTP
            |> 3.) Settings
            |> 4.) Exit""".stripMargin)
      }

      promptMessage()
      currCommand = StdIn.readLine()
      currCommand match {
        case "1" if !loggedIn => login()
        case "1" if loggedIn => logout()
        case "2" if !loggedIn => quit(); break = true
        case "2" => startRCTP()
        case "3" if loggedIn && getAdminStatus(email) => settings(status = "admin")
        case "3" if loggedIn => settings(status = "basic")
        case "4" if loggedIn => quit(); break = true
        case _ => println("Invalid Option. Enter a valid number option."); currCommand += "!"
      }

      clearScreen()
    } while(!break)
    getSparkSession.close()
  }
}
