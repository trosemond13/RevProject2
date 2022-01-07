import scala.Console.{BOLD, GREEN, RED, RESET, print, println}
import com.roundeights.hasher.Implicits._
import scala.io.StdIn

class RevCTP extends HiveDBC {
  protected val SUCCESS = 0
  protected val FAILURE = 1
  protected val ERROR = -1
  protected var employees: Map[String, (String, Long, String, String, Boolean)] = getEmployees(deleted = false)
  protected val init = if(employees.contains("root@rctp.com")) false else true
  protected var email:String = ""
  protected var loggedIn: Boolean = false

  protected def login(): Int = {
    var tries: Int = 3
    while(!loggedIn) {
      print(s"$GREEN${BOLD}LOGIN MENU$RESET> Enter your email here -> ")
      email = StdIn.readLine().toLowerCase().trim
      print(s"$GREEN${BOLD}LOGIN MENU$RESET> Enter your password here -> ")
      val password: String = StdIn.readLine().trim.sha256.hash
      if(employees.contains(email) && employees(email)._1 == password) {
        println(s"$GREEN${BOLD}LOGIN MENU$RESET> Logged in as <" + email.split('@')(0) + ">")
        loggedIn = true
        return SUCCESS
      } else {
        tries = tries - 1
        if(tries>0) {
          println(s"$RED${BOLD}LOGIN FAILURE$RESET> Incorrect email or password. " + tries + " more tries left or the program will exit!")
        } else {
          println(s"$RED${BOLD}SYSTEM$RESET> You typed in the incorrect email or password too many times. Exiting login page...")
          email = ""
          return FAILURE
        }
      }
    }
    FAILURE
  }

  protected def updateEmployeeInfo(terminate: Boolean, employee_id: Boolean, first_name: Boolean, last_name: Boolean, email: Boolean, password: Boolean, admin: Boolean): Unit = {
    //super.updateEmployeeInfo(terminate, employee_id, first_name, last_name, email, password, admin)
  }
  protected def checkEmail(e: String): Boolean = {
    val emailRegex = """^[a-zA-Z0-9\.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$""".r
    e match {
      case null => false
      case e if e.trim.isEmpty => false
      case e if emailRegex.findFirstMatchIn(e).isDefined => true
      case _ => false
    }
  }

}
object Main extends RevCTP {
  val rctp = new RevCTP()

  if(rctp.init) {
    println(s"$GREEN${BOLD}INITIALIZATION$RESET> Welcome to the Revature Covid Tracker Planning app. We appreciate your new subscription to our service.")
    println(s"$GREEN${BOLD}INITIALIZATION$RESET> Please sign in with the default login to initialize your employee database.")
    if(login() == FAILURE) {
      println(s"$RED${BOLD}INITIALIZATION$RESET> Initialization failed due to login failure. Please restart software and try again!")
      println(s"$GREEN${BOLD}SYSTEM$RESET> Exiting application...")
    } else {

    }
  }
}