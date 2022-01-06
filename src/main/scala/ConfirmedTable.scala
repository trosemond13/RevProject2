import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
object ConfirmedTable {
  def main(args:Array[String]): Unit ={
    val session = new ConfirmedTable()
    session.init()
    session.createTable()
    session.showTables()
    session.demoConfirmed()
    session.close()
  }

  class ConfirmedTable () {
    private var spark: SparkSession = null

    def init(): Unit = {
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      spark = SparkSession
      .builder
      .appName("ConfirmedTable")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nsession created\n\n\n\n\n\n")
    }

    private def createDF(): DataFrame = {
      spark.read.format("csv").option("header", "true").load("time_series_covid_19_confirmed_US.csv")
    }

    def createTable(): Unit = {
      val df = createDF()
      if (!spark.catalog.tableExists("confirmed")) {
        println("\ncreating table\n")
        df.write.saveAsTable("confirmed")
      }
      else
        println("table already there")
    }

    def showTables(): Unit = {
      spark.sql("show tables").show
    }

    def demoConfirmed(): Unit = {
      spark.sql("select admin2,combined_key from confirmed limit 10").show
    }

    def close(): Unit = {
      spark.close()
    }
  }
}
