package com.data

import com.Main.promptMessage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.io.StdIn
import scala.io.AnsiColor.{RESET, UNDERLINED}
import com.tools.Router.dbCon

import scala.Console.print

object RecoveryData {
  var isFinishedR = false

  // Sub Menu For Recovery Data
  def recovery_options_menu(): Unit = {
    while(!isFinishedR) {
      println(s"""
                 |${UNDERLINED}MAIN/START RCTP/Recovery Data Menu: Please select one of the following menu options.${RESET}
                 |
                 |--> 1.) US Total Recovered Patients
                 |--> 2.) Recovered Patients By State
                 |--> 3.) Return To Main""".stripMargin)
      promptMessage()
      val recoverySelector = StdIn.readLine()

      if(recoverySelector == "1")
      {return_recovered_amt()}
      else if(recoverySelector == "2")
      {return_states_recovered()}
      else if(recoverySelector == "3") {
        println("Return To Main")
        isFinishedR = true
      }
      else
      {println("Invalid Option")}
    }
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

    println("\nRecovery Rates By State\n")

    dbCon.sql(
      """
      SELECT ProvinceState, CAST((Max(Confirmed) - Max(Deaths)) AS INT) AS PatientsRecovered FROM StatesList
      WHERE CountryRegion = 'US'
        AND ProvinceState NOT LIKE '%,%'
        AND ProvinceState != 'US'
        AND ProvinceState != 'Recovered'
      GROUP BY ProvinceState ORDER BY ProvinceState
      """).show(Int.MaxValue, false)

    print("Enter Any Key To Return")
    StdIn.readLine()
  }

  // Return Overall Recovered US Covid Patients
  def return_recovered_amt(): Unit = {
    val ds = create_df().withColumn("TotalRecovered", col("Confirmed") - col("Deaths"))

    val recNum = ds.select(col("TotalRecovered"))
      .filter(col("Country/Region") === "US"
        && col("ObservationDate") === "05/29/2021"
        && col("Province/State").notEqual("US")
        && col("Province/State").notEqual("Recovered"))
      .agg(sum(col("TotalRecovered")))

    println("\nPatients Recovered: " + recNum.first()(0).asInstanceOf[Double].toLong)

    print("Enter Any Key To Return")
    StdIn.readLine()
  }
}
