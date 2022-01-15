package com.data

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.StdIn
import com.tools.Router.dbCon

object RecoveryData {
  // Sub Menu For Recovery Data
  def recovery_options_menu(): Unit = {
    println("""
        |What Would You Like To See?
        |RECOVERY MENU> 1.) US Total Recovered Patients
        |RECOVERY MENU> 2.) Recovered Patients By State
        |RECOVERY MENU> 3.) Return To Main""".stripMargin)
    val recoverySelector = StdIn.readLine()

    if(recoverySelector == "1")
    {println("\nPatients Recovered: " + return_recovered_amt().asInstanceOf[Double].toLong)}
    else if(recoverySelector == "2")
    {return_states_recovered()}
    else if(recoverySelector == "3")
    {println("Return To Main")}
    else
    {println("Invalid Option")}
  }

  // Create DataFrame From Complete Kaggle CSV
  def create_df(): DataFrame = {
    val df = dbCon.read.format("csv")
      .option("header", "true")
      .options(Map("inferSchema"->"true","delimiter"->","))
      .load("KaggleData(Complete)\\KaggleData(Complete)\\covid_19_data_complete(Kaggle).csv")

    return df
  }

  // Show List Of Recovered Patients Per State
  def return_states_recovered(): Unit = {
    val ds = create_df()
      .withColumnRenamed("Country/Region", "CountryRegion")
      .withColumnRenamed("Province/State","ProvinceState")
      .withColumn("TotalRecovered", col("Confirmed") - col("Deaths"))

    ds.createOrReplaceTempView("StatesList")

    dbCon.sql(
      """
      SELECT ProvinceState, CAST((Max(Confirmed) - Max(Deaths)) AS INT) AS PatientsRecovered FROM StatesList
      WHERE CountryRegion = 'US'
        AND ProvinceState NOT LIKE '%,%'
        AND ProvinceState != 'US'
        AND ProvinceState != 'Recovered'
      GROUP BY ProvinceState ORDER BY ProvinceState
      """)
      .show(Int.MaxValue, false)
  }

  // Return Overall Recovered US Covid Patients
  def return_recovered_amt(): Any = {
    val ds = create_df().withColumn("TotalRecovered", col("Confirmed") - col("Deaths"))

    val recNum = ds.select(col("TotalRecovered"))
      .filter(col("Country/Region") === "US"
        && col("ObservationDate") === "05/29/2021"
        && col("Province/State").notEqual("US")
        && col("Province/State").notEqual("Recovered"))
      .agg(sum(col("TotalRecovered")))

    /*val recNum = ds.select(col("Province/State"), col("Recovered"), col("TotalRecovered"))
    .filter(col("Country/Region") === "US"
      && col("Province/State") === "Recovered"
      && col("Recovered").notEqual(0.0))
    .sort(col("Recovered").desc)
    .limit(1)*/

    return recNum.first()(0)
  }
}
