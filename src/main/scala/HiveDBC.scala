import org.apache.spark.sql.{DataFrame, SparkSession}

class HiveDBC {
  var spark: SparkSession = null

  private def executeDML(spark: SparkSession, query : String): Unit = {
    spark.sql(query).queryExecution
  }

  private def executeQuery(spark: SparkSession, query: String): DataFrame = {
    spark.sql(query)
  }

  private def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach{Logger.getLogger(_).setLevel(Level.OFF)}
  }

  def getSparkSession(): SparkSession = {
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
      executeDML(spark, "CREATE TABLE IF NOT EXISTS employees(employee_id Int, email String, password String, admin Boolean, deleted Boolean)")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS recovered()")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS deaths()")
      //executeDML(spark, "CREATE TABLE IF NOT EXISTS confirmed()")



    }
    spark
  }

}
