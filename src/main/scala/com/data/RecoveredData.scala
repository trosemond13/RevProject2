package com.data

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import scala.io.StdIn

object RecoveredData {
  var con: SparkSession = null

  def initializer(): Unit = {
    con = SparkSession
      .builder()
      .appName("Covid")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    con.sparkContext.setLogLevel("ERROR")
  }

  def create_df(): DataFrame = {
    val df = con.read.format("csv")
      .option("header", "true")
      .options(Map("inferSchema"->"true","delimiter"->","))
      .load("D:\\Projects\\Proj30\\prod\\RevProject2\\KaggleData(Complete)\\KaggleData(Complete)\\covid_19_data_complete(Kaggle).csv")

    return df
  }

  def ret_recovered_amt(): Any = {
    val ds = create_df()

    val recNum = ds.select(col("Province/State"), col("Recovered"))
      .filter(col("Country/Region") === "US"
        && col("Province/State") === "Recovered"
        && col("Recovered").notEqual(0.0))
      .sort(col("Recovered").desc)
      .limit(1)

    return recNum.first()(1)
  }

  def closer(): Unit = {
    con.close()
  }
}
