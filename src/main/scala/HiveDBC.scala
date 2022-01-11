import org.apache.spark.sql.{DataFrame, SparkSession}
import com.roundeights.hasher.Implicits._

class HiveDBC {
  var spark: SparkSession = null

  /**
   * This method performs the following data manipulation language stored in query.
   * @param spark The spark session
   * @param query This match criteria that is ran against the database.
   */
  private def executeDML(spark: SparkSession, query : String): Unit = {
    spark.sql(query).queryExecution
  }

  /**
   * This method returns the records that match the query information as a DataFrame.
   * @param spark The spark session
   * @param query This match criteria that is ran against the database.
   * @return returns the matched criteria as the instance of a DataFrame
   */
  private def executeQuery(spark: SparkSession, query: String): DataFrame = {
    spark.sql(query)
  }

  /**
   * This methods stops the specified warnings from being printed to standard output.
   * @param params The list of dependency string names desired to be ignored.
   */
  private def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach{Logger.getLogger(_).setLevel(Level.OFF)}
  }
  /**
   * This method returns the current spark session. If one does not exist, a new spark session is made.
   * @return spark: returns the new spark session as an instance of a SparkSession.
   */
  protected def getSparkSession(): SparkSession = {
    if(spark == null) {
      suppressLogs(List("org", "akka"))
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      spark = SparkSession
        .builder()
        .appName("RevProject2")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      spark.sparkContext.setLogLevel("ERROR")
      executeDML(spark, "CREATE DATABASE IF NOT EXISTS RevCTP")
      executeDML(spark, "USE RevCTP")
      executeDML(spark, "CREATE TABLE IF NOT EXISTS employees(employee_id Long, first_name String, last_name String, email String, password String, admin Boolean, deleted Boolean)")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS recovered()")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS deaths()")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS confirmed()")
    }
    spark
  }

  /**
   * This method generates the next available employee number. This replaces the auto_increment feature that is
   * not available in HIVE.
   * @return returns the next available employee number as an instance of a Long.
   */
  protected def generateNewEmployeeID(): Long = {
    val spark = getSparkSession()
    val employees = executeQuery(spark, "SELECT * FROM employees")
    employees.count() + 8253
  }

  /**
   * This method pulls the specified employee data from the database, and puts it in a collection.
   * @param deleted If set to true, active employees' information will be pulled. Else, inactive employees' information will be pulled.
   * @return employeesMap: returns the employee information as an instance of Map[String, (String, Long, String, String, Boolean)]
   */
  protected def getEmployees(deleted: Boolean): Map[String, (String, Long, String, String, Boolean)] = {
    val spark = getSparkSession()
    val employees = executeQuery(spark, s"SELECT * FROM employees WHERE deleted == $deleted")
    var employeesMap: Map[String, (String, Long, String, String, Boolean)] = Map[String, (String, Long, String, String, Boolean)]()

    //Reads the employee information from the hive, and puts it in a map collection.
    employees.collect().foreach(row => {
      val employee_id = row.getLong(0)
      val first_name = row.getString(1)
      val last_name = row.getString(2)
      val email = row.getString(3)
      val password = row.getString(4)
      val admin = row.getBoolean(5)
      employeesMap += email -> (password, employee_id, first_name, last_name, admin)
    })
    employeesMap
  }

  protected def getEmailDomain(): String = {
    val spark = getSparkSession()
    val employees = executeQuery(spark, s"SELECT email FROM employees WHERE employee_id = 8253")

    employees.collect().head.getString(0).split('@')(1)
  }

  /**
   * This method allows for users to be created. The created user will be added into the Hive database.
   * @param employee_id The employee's number in the system. (Primary Key)
   * @param first_name The employee's first name.
   * @param last_name The employee's last name.
   * @param email The employee's email. This field is treated as the username.
   * @param password The employee's password. (Encrypted).
   * @param admin If set to true, the user will have admin privileges. Else, the user will be a basic user.
   */
  protected def createEmployee(employee_id: Long, first_name: String, last_name: String, email: String, password: String, admin: Boolean): Unit = {
    executeDML(spark, s"insert into employees values ($employee_id, '$first_name', '$last_name', '$email', '$password', $admin, false)")
  }

  /**
   * This method allows for users to be softly deleted, reinstated, or for any other information to be updated
   * @param terminate If set to true, the user's access to the software is terminated. Else, the user is reinstated and gains access to the software.
   * @param employee_id The employee's number in the system. (Primary Key)
   * @param first_name The employee's desired first name.
   * @param last_name The employee's desired last name.
   * @param email The employee's email. This field is treated as the username.
   * @param password The employee's desired password. (Encrypted).
   * @param admin If set to true, the user will have admin privileges. Else, the user will be a basic user.
   */
  protected def updateEmployeeInfo(terminate:Boolean, employee_id: Long, first_name: String, last_name: String, email: String, password: String, admin: Boolean): Unit = {
    val spark = getSparkSession()
    val employees = executeQuery(spark, s"SELECT * FROM employees WHERE employee_id != $employee_id")
    executeDML(spark, "CREATE TABLE tempemployees(employee_id Long, first_name String, last_name String, email String, password String, admin Boolean, deleted Boolean)")

    employees.collect().foreach(row => {
      val employee_id = row.getLong(0)
      val first_name = row.getString(1)
      val last_name = row.getString(2)
      val email = row.getString(3)
      val password = row.getString(4)
      val admin = row.getBoolean(5)
      val deleted = row.getBoolean(6)
      executeDML(spark, s"insert into tempemployees values ($employee_id, '$first_name', '$last_name', '$email', '$password', $admin, $deleted)")
    })
    executeDML(spark, s"insert into tempemployees values ($employee_id, '$first_name', '$last_name', '$email', '$password', $admin, $terminate)")
    executeDML(spark, "DROP TABLE employees")
    executeDML(spark, "ALTER TABLE tempemployees RENAME TO employees")
  }
}
