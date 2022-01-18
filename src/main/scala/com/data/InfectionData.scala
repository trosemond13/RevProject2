package com.data

import com.Main.promptMessage

import scala.io.StdIn
import com.tools.Router.dbCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.Console.print
import scala.io.AnsiColor.{RESET, UNDERLINED}

object InfectionData {
  var isFinishedI = false

  // Sub Menu For Infections Data
  def infections_menu(): Unit = {
    while(!isFinishedI) {
      println(s"""
                 |${UNDERLINED}MAIN/START RCTP/Infection Data Menu: Please select one of the following menu options.${RESET}
                 |
                 |--> 1.) Highest Infection Rates By State
                 |--> 2.) Total US Infections
                 |--> 3.) Average Infections Per Week
                 |--> 4.) Average Infections Per Week Per State
                 |--> 5.) Return To Main""".stripMargin)
      promptMessage()
      val infectionsSelector = StdIn.readLine()

      if(infectionsSelector == "1")
      {rank_states_infections()}
      else if(infectionsSelector == "2")
      {overall_infections()}
      else if(infectionsSelector == "3")
      {average_weekly_infections()}
      else if (infectionsSelector == "4")
      {week_by_state()}
      else if(infectionsSelector == "5") {
        isFinishedI = true
      }
      else
      {println("Invalid Option")}
    }
  }

  // Create Initial DataFrame For Infections
  def create_df(): DataFrame = {
    val df = dbCon.read.format("csv")
      .option("header", "true")
      .options(Map("inferSchema"->"true","delimiter"->","))
      .load("KaggleData(Complete)\\KaggleData(Complete)\\time_series_covid_19_confirmed_US_complete(Kaggle).csv")

    df
  }

  // Rank All States By Infection Numbers
  def rank_states_infections(): Unit = {
    val rankingStatesDf = create_df().select(col("Admin2"), col("Province_State"), col("5/29/21"))
      .withColumnRenamed("5/29/21", "05_29_2021_I")

    rankingStatesDf.createOrReplaceTempView("StatesRanked")

    dbCon.sql(
      """
      SELECT Province_State AS State, SUM(05_29_2021_I) AS Infections
      FROM StatesRanked
      WHERE Province_State NOT LIKE("%Princess%")
      GROUP BY Province_State ORDER BY Infections DESC
      """)show(Int.MaxValue, false)

    print("Enter Any Key To Return")
    StdIn.readLine()
  }

  // Breakdown Average Infection By State
  def week_by_state(): Unit = {
    println("\nAverage Weekly Infections By State\n")
    val avgByStateDf = create_df().select(col("Province_State"), round((col("5/29/21") / 494), 0) * 7)
      .where(!col("Province_State").contains("Princess"))
      .withColumnRenamed("(round((5/29/21 / 494), 0) * 7)", "Infections")
      .groupBy(col("Province_State")).sum("Infections")
      .withColumnRenamed("sum(Infections)", "Infections Per Week")
      .orderBy(col("sum(Infections)").desc)

    avgByStateDf.show(Int.MaxValue, false)

    print("Enter Any Key To Return")
    StdIn.readLine()
  }

  // Average Weekly Infections By State
  def average_weekly_infections(): Unit = {
    val avgDf = create_df().select((sum(col("5/29/21") / 494)) * 7)
      .withColumnRenamed("(sum((5/29/21 / 494)) * 7)", "Infections Per Week")
      .collect()

    println("\nUS Average Infections Per Week: \n" + avgDf(0)(0).asInstanceOf[Double].toInt)
    print("Enter Any Key To Return")
    StdIn.readLine()
  }

  // Overall US Infections
  def overall_infections(): Unit = {
    val overallDf = create_df().select(sum(col("5/29/21")))
    println("\nTotal US Confirmed Infections: \n" + overallDf.first()(0))

    print("Enter Any Key To Return")
    StdIn.readLine()
  }
}
