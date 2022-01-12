package com.data

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import scala.io.StdIn
import com.tools.Router.dbCon

object RecoveryData {
  def create_df(): DataFrame = {
    val df = dbCon.read.format("csv")
      .option("header", "true")
      .options(Map("inferSchema"->"true","delimiter"->","))
      .load("KaggleData(Complete)\\KaggleData(Complete)\\covid_19_data_complete(Kaggle).csv")

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
}
