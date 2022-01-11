import org.apache.spark.sql.SparkSession

class HiveDBC {
  var spark: SparkSession = null

  private def supressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach{Logger.getLogger(_).setLevel(Level.OFF)}
  }
}
